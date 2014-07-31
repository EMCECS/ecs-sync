/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.atmos.sync.util;

import com.emc.atmos.sync.AtmosSync2;
import com.emc.atmos.sync.Timeable;
import com.emc.atmos.sync.plugins.SourcePlugin;
import com.emc.atmos.sync.plugins.SyncPlugin;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Callable;

public final class TimingUtil {
    private static final Logger log = Logger.getLogger( TimingUtil.class );

    private static Map<SyncPlugin, Timings> registry = new Hashtable<SyncPlugin, Timings>();

    /**
     * registers all plug-ins of the given sync instance so that they are all associated with the same timing group.
     */
    public static synchronized void register( AtmosSync2 sync, int timingWindow ) {
        Timings timings = new WindowedTimings( timingWindow );
        registry.put( sync.getSource(), timings );
        for ( SyncPlugin plugin : sync.getPluginChain() ) {
            registry.put( plugin, timings );
        }
        registry.put( sync.getDestination(), timings );
    }

    public static void startOperation( SyncPlugin plugin, String name ) {
        getTimings( plugin ).startOperation( plugin.getName() + "::" + name );
    }

    public static void completeOperation( SyncPlugin plugin, String name ) {
        getTimings( plugin ).completeOperation( plugin.getName() + "::" + name );
    }

    public static void failOperation( SyncPlugin plugin, String name ) {
        getTimings( plugin ).failOperation( plugin.getName() + "::" + name );
    }

    public static <T> T time(SyncPlugin plugin, String name, Timeable<T> timeable) {
        startOperation(plugin, name);
        try {
            T t = timeable.call();
            completeOperation(plugin, name);
            return t;
        } catch (RuntimeException e) {
            failOperation(plugin, name);
            throw e;
        }
    }

    public static <T> T time(SyncPlugin plugin, String name, Callable<T> timeable) throws Exception {
        startOperation(plugin, name);
        try {
            T t = timeable.call();
            completeOperation(plugin, name);
            return t;
        } catch (Exception e) {
            failOperation(plugin, name);
            throw e;
        }
    }

    public static void logTimings( SourcePlugin source ) {
        getTimings( source ).dump();
    }

    private static Timings getTimings( SyncPlugin plugin ) {
        Timings timings = registry.get( plugin );
        if ( timings == null ) timings = NULL_TIMINGS;
        return timings;
    }

    private TimingUtil() {
    }

    private static interface Timings {
        public void startOperation( String name );

        public void completeOperation( String name );

        public void failOperation( String name );

        public void dump();
    }

    private static class WindowedTimings implements Timings {
        private ThreadLocal<Map<String, Long>> operationStartTimes = new ThreadLocal<Map<String, Long>>();
        private final Map<String, Long> operationMinTimes = new Hashtable<String, Long>();
        private final Map<String, Long> operationMaxTimes = new Hashtable<String, Long>();
        private final Map<String, Long> operationGrossTimes = new Hashtable<String, Long>();
        private final Map<String, Long> operationCompleteCounts = new Hashtable<String, Long>();
        private final Map<String, Long> operationFailedCounts = new Hashtable<String, Long>();

        private int statsWindow;
        private boolean dumpPending;
        private long collectionStartTime;

        public WindowedTimings( int statsWindow ) {
            this.statsWindow = statsWindow;
            collectionStartTime = System.currentTimeMillis();
        }

        public void startOperation( String name ) {
            getOperationStartTimes().put( name, System.currentTimeMillis() );
        }

        public void completeOperation( String name ) {
            long time = endAndTimeOperation( name ), complete, count;
            boolean dump = false;
            synchronized ( this ) {
                complete = getValue( operationCompleteCounts, name );
                count = complete + getValue( operationFailedCounts, name );
                checkMinMaxTime( name, time );
                operationGrossTimes.put( name, getValue( operationGrossTimes, name ) + time );
                operationCompleteCounts.put( name, complete + 1 );
                if ( count >= statsWindow && !dumpPending ) {
                    dumpPending = true;
                    dump = true;
                }
            }
            if ( dump ) dump();
        }

        public void failOperation( String name ) {
            long time = endAndTimeOperation( name ), failed, count;
            boolean dump = false;
            synchronized ( this ) {
                failed = getValue( operationFailedCounts, name );
                count = failed + getValue( operationCompleteCounts, name );
                checkMinMaxTime( name, time );
                operationGrossTimes.put( name, getValue( operationGrossTimes, name ) + time );
                operationFailedCounts.put( name, failed + 1 );
                if ( count >= statsWindow && !dumpPending ) {
                    dumpPending = true;
                    dump = true;
                }
            }
            if ( dump ) dump();
        }

        public void dump() {
            List<TimingStats> stats = new ArrayList<TimingStats>();
            synchronized ( this ) {
                for ( String name : operationGrossTimes.keySet() ) {
                    stats.add( new TimingStats( name,
                                                getValue( operationCompleteCounts, name ),
                                                getValue( operationFailedCounts, name ),
                                                getValue( operationMinTimes, name ),
                                                getValue( operationMaxTimes, name ),
                                                getValue( operationGrossTimes, name ) ) );
                }
                operationMinTimes.clear();
                operationMaxTimes.clear();
                operationGrossTimes.clear();
                operationCompleteCounts.clear();
                operationFailedCounts.clear();
                dumpPending = false;
            }
            long now = System.currentTimeMillis();
            log.info( "Start timings dump (" + (now - collectionStartTime)
                      + "ms since last dump)\n######################################################################" );
            Collections.sort( stats );
            for ( TimingStats stat : stats ) {
                log.info( stat );
            }
            log.info( "End timings dump\n######################################################################" );
            collectionStartTime = now;
        }

        private Map<String, Long> getOperationStartTimes() {
            Map<String, Long> map = operationStartTimes.get();
            if ( map == null ) {
                map = new HashMap<String, Long>();
                operationStartTimes.set( map );
            }
            return map;
        }

        private long getValue( Map<String, Long> map, String key ) {
            Long value = map.get( key );
            if ( value == null ) value = 0L;
            return value;
        }

        private long endAndTimeOperation( String name ) {
            Long startTime = getOperationStartTimes().get( name );
            if ( startTime == null )
                throw new IllegalStateException( "no start time exists for operation " + name );
            return System.currentTimeMillis() - startTime;
        }

        private void checkMinMaxTime( String name, long time ) {
            Long min = operationMinTimes.get( name );
            if ( min == null || min > time ) operationMinTimes.put( name, time );
            Long max = operationMaxTimes.get( name );
            if ( max == null || max < time ) operationMaxTimes.put( name, time );
        }
    }

    private static class TimingStats implements Comparable<TimingStats> {
        private String name;
        private long completeCount;
        private long failedCount;
        private long minTime;
        private long maxTime;
        private long grossTime;

        public TimingStats( String name, long completeCount, long failedCount,
                            long minTime, long maxTime, long grossTime ) {
            this.name = name;
            this.completeCount = completeCount;
            this.failedCount = failedCount;
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.grossTime = grossTime;
        }

        public String getName() {
            return name;
        }

        public long getCompleteCount() {
            return completeCount;
        }

        public long getFailedCount() {
            return failedCount;
        }

        public long getMinTime() {
            return minTime;
        }

        public long getMaxTime() {
            return maxTime;
        }

        public long getGrossTime() {
            return grossTime;
        }

        @Override
        public int compareTo( TimingStats o ) {
            return name.compareTo( o.getName() );
        }

        @Override
        public String toString() {
            long totalCount = completeCount + failedCount;
            long avg = grossTime / (totalCount == 0 ? 1 : totalCount);
            return name + '\n'
                   + "    Completed:" + rAlign( Long.toString( completeCount ), 6 )
                   + "    Failed:" + rAlign( Long.toString( failedCount ), 6 )
                   + "    Min/Max/Avg Time:" + rAlign( Long.toString( minTime ), 4 )
                   + "/" + rAlign( Long.toString( maxTime ), 4 )
                   + "/" + rAlign( Long.toString( avg ), 4 )
                   + "ms";
        }

        private String rAlign( String string, int length ) {
            return String.format( "%1$" + length + "s", string );
        }
    }

    /**
     * Used when timings are disabled
     */
    private static Timings NULL_TIMINGS = new Timings() {
        @Override
        public void startOperation( String name ) {
        }

        @Override
        public void completeOperation( String name ) {
        }

        @Override
        public void failOperation( String name ) {
        }

        @Override
        public void dump() {
        }
    };
}

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
package com.emc.atmos.sync.util.test;

import com.emc.atmos.sync.AtmosSync2;
import com.emc.atmos.sync.Timeable;
import com.emc.atmos.sync.plugins.DummyDestination;
import com.emc.atmos.sync.plugins.SourcePlugin;
import com.emc.atmos.sync.plugins.SyncObject;
import com.emc.atmos.sync.plugins.SyncPlugin;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TimingUtilTest {
    @Test
    public void testTimings() {
        int threadCount = Runtime.getRuntime().availableProcessors() * 16; // 16 threads per core for stress
        int window = threadCount * 100; // should dump stats every 100 "objects" per thread
        int total = window * 5; // ~500 "objects" per thread total

        DummyPlugin plugin = new DummyPlugin();

        AtmosSync2 sync = new AtmosSync2();
        sync.setSource( new DummySource( threadCount, total ) );
        sync.setPluginChain( Arrays.asList( (SyncPlugin) plugin ) );
        sync.setDestination( new DummyDestination() );
        sync.setTimingsEnabled( true );
        sync.setTimingWindow( window );
        sync.afterPropertiesSet();

        sync.run();

        System.out.println( "---Timing enabled---" );
        System.out.println( "Per-thread overhead is " + (plugin.getOverhead() / threadCount) + "ms over 500 calls" );
        System.out.println( "Per-call overhead is " + ((plugin.getOverhead() * 1000) / (total)) + "µs" );

        plugin = new DummyPlugin(); // this one won't be registered

        sync.setPluginChain( Arrays.asList( (SyncPlugin) plugin ) );
        sync.setTimingsEnabled( false );
        sync.afterPropertiesSet();

        sync.run();

        System.out.println( "---Timing disabled---" );
        System.out.println( "Per-thread overhead is " + (plugin.getOverhead() / threadCount) + "ms over 500 calls" );
        System.out.println( "Per-call overhead is " + ((plugin.getOverhead() * 1000) / (total)) + "µs" );
    }

    private class DummySource extends SourcePlugin {
        private int threadCount, totalCount;

        public DummySource( int threadCount, int totalCount ) {
            this.threadCount = threadCount;
            this.totalCount = totalCount;
        }

        @Override
        public void run() {
            ThreadPoolExecutor executor = new ThreadPoolExecutor( threadCount, threadCount, 5, TimeUnit.SECONDS,
                                                                  new LinkedBlockingQueue<Runnable>( totalCount ) );
            for ( int i = 0; i < totalCount; i++ ) {
                executor.execute( new Runnable() {
                    @Override
                    public void run() {
                        getNext().filter( new DummySyncObject() );
                    }
                } );
            }

            while ( true ) {
                try {
                    Thread.sleep( 500 );
                } catch ( InterruptedException e ) {
                    e.printStackTrace();
                }
                if ( executor.getActiveCount() == 0 ) {
                    executor.shutdown();
                    return;
                }
            }
        }

        @Override
        public void terminate() {
        }

        @Override
        public void printStats() {
        }

        @Override
        public Options getOptions() {
            return null;
        }

        @Override
        public boolean parseOptions( CommandLine line ) {
            return false;
        }

        @Override
        public void validateChain( SyncPlugin first ) {
        }

        @Override
        public String getName() {
            return "Dummy Source";
        }

        @Override
        public String getDocumentation() {
            return null;
        }
    }

    private class DummyPlugin extends SyncPlugin {
        private long overhead = 0;

        @Override
        public void filter( SyncObject obj ) {
            long start = System.currentTimeMillis();
            time( new Timeable<Void>() {
                @Override
                public Void call() {
                    return null;
                }
            }, "No-op" );
            long overhead = System.currentTimeMillis() - start;
            addOverhead( overhead );
        }

        @Override
        public Options getOptions() {
            return null;
        }

        @Override
        public boolean parseOptions( CommandLine line ) {
            return false;
        }

        @Override
        public void validateChain( SyncPlugin first ) {
        }

        @Override
        public String getName() {
            return "Dummy Plugin";
        }

        @Override
        public String getDocumentation() {
            return null;
        }

        public long getOverhead() {
            return this.overhead;
        }

        private synchronized void addOverhead( long ms ) {
            overhead += ms;
        }
    }

    private class DummySyncObject extends SyncObject {
        @Override
        public InputStream getInputStream() {
            return null;
        }

        @Override
        public String getRelativePath() {
            return null;
        }

        @Override
        public long getBytesRead() {
            return 0;
        }
    }
}

/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.util;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.DummyTarget;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.model.object.AbstractSyncObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.Test;

import java.io.InputStream;
import java.util.*;

public class TimingUtilTest {
    // NOTE: timing window requires manual verification of the log output
    @Test
    public void testTimings() {
        int threadCount = Runtime.getRuntime().availableProcessors() * 8; // 8 threads per core for stress
        int window = threadCount * 100; // should dump stats every 100 "objects" per thread
        int total = window * 5; // ~500 "objects" per thread total

        DummyFilter filter = new DummyFilter();

        EcsSync sync = new EcsSync();
        sync.setSource(new DummySource(total));
        sync.setFilters(Collections.singletonList((SyncFilter) filter));
        sync.setTarget(new DummyTarget());
        sync.setTimingsEnabled(true);
        sync.setTimingWindow(window);
        sync.setSyncThreadCount(threadCount);

        sync.run();

        System.out.println("---Timing enabled---");
        System.out.println("Per-thread overhead is " + (filter.getOverhead() / threadCount / 1000000) + "ms over 500 calls");
        System.out.println("Per-call overhead is " + ((filter.getOverhead()) / (total) / 1000) + "µs");

        filter = new DummyFilter(); // this one won't be registered

        sync.setFilters(Collections.singletonList((SyncFilter) filter));
        sync.setTimingsEnabled(false);

        sync.run();

        System.out.println("---Timing disabled---");
        System.out.println("Per-thread overhead is " + (filter.getOverhead() / threadCount / 1000000) + "ms over 500 calls");
        System.out.println("Per-call overhead is " + ((filter.getOverhead()) / (total) / 1000) + "µs");
    }

    private class DummySource extends SyncSource<DummySyncObject> {
        private List<DummySyncObject> objects;

        public DummySource(int totalCount) {
            objects = new ArrayList<>();
            for (int i = 0; i < totalCount; i++) {
                objects.add(new DummySyncObject());
            }
        }

        @Override
        public Iterator<DummySyncObject> iterator() {
            return objects.iterator();
        }

        @Override
        public Iterator<DummySyncObject> childIterator(DummySyncObject syncObject) {
            return null;
        }

        @Override
        public boolean canHandleSource(String sourceUri) {
            return false;
        }

        @Override
        public Options getCustomOptions() {
            return new Options();
        }

        @Override
        protected void parseCustomOptions(CommandLine line) {
        }

        @Override
        public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
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

    private class DummyFilter extends SyncFilter {
        private long overhead = 0;

        @Override
        public void filter(SyncObject obj) {
            long start = System.nanoTime();
            time(new Function<Void>() {
                @Override
                public Void call() {
                    return null;
                }
            }, "No-op");
            long overhead = System.nanoTime() - start;
            addOverhead(overhead);
        }

        @Override
        public SyncObject reverseFilter(SyncObject obj) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getActivationName() {
            return null;
        }

        @Override
        public Options getCustomOptions() {
            return new Options();
        }

        @Override
        protected void parseCustomOptions(CommandLine line) {
        }

        @Override
        public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        }

        @Override
        public String getName() {
            return "Dummy Filter";
        }

        @Override
        public String getDocumentation() {
            return null;
        }

        public long getOverhead() {
            return this.overhead;
        }

        private synchronized void addOverhead(long ns) {
            overhead += ns;
        }
    }

    private class DummySyncObject extends AbstractSyncObject<String> {
        public DummySyncObject() {
            super("dummy", "dummy", "dummy", false);
        }

        @Override
        protected InputStream createSourceInputStream() {
            return null;
        }

        @Override
        protected void loadObject() {
        }
    }
}

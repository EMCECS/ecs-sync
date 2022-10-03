/*
 * Copyright (c) 2014-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.util;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.filter.AbstractFilter;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.test.TestUtil;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

public class TimingUtilTest {
    // NOTE: timing window requires manual verification of the log output
    @Test
    public void testTimings() {
        int threadCount = Runtime.getRuntime().availableProcessors() * 2; // 2 threads per core for stress
        int window = threadCount * 100; // should dump stats every 100 "objects" per thread
        int total = window * 10; // 1000 objects per thread total

        TestConfig testConfig = new TestConfig();
        testConfig.setObjectCount(total);
        testConfig.setMaxSize(1024);
        testConfig.setChanceOfChildren(0);
        testConfig.setReadData(false);
        testConfig.setDiscardData(false);

        NoOpConfig noOpConfig = new NoOpConfig();

        SyncOptions options = new SyncOptions().withTimingsEnabled(true).withTimingWindow(window).withThreadCount(threadCount);

        SyncConfig syncConfig = new SyncConfig().withSource(testConfig).withTarget(testConfig)
                .withFilters(Collections.singletonList(noOpConfig)).withOptions(options);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        TestUtil.run(sync);

        System.out.println("---Timing enabled---");
        System.out.println("Per-thread overhead is " + (noOpConfig.getOverhead() / threadCount / 1000000) + "ms over 500 calls");
        System.out.println("Per-call overhead is " + ((noOpConfig.getOverhead()) / (total) / 1000) + "µs");

        // now disable timings
        noOpConfig = new NoOpConfig();
        syncConfig.setFilters(Collections.singletonList(noOpConfig));
        options.setTimingsEnabled(false);

        sync = new EcsSync(); // cannot re-use an EcsSync instance
        sync.setSyncConfig(syncConfig);
        TestUtil.run(sync);

        System.out.println("---Timing disabled---");
        System.out.println("Per-thread overhead is " + (noOpConfig.getOverhead() / threadCount / 1000000) + "ms over 500 calls");
        System.out.println("Per-call overhead is " + ((noOpConfig.getOverhead()) / (total) / 1000) + "µs");
    }

    public static class NoOpFilter extends AbstractFilter<NoOpConfig> {
        @Override
        public void filter(ObjectContext objectContext) {
            long start = System.nanoTime();
            time((Function<Void>) () -> null, "No-op");
            long overhead = System.nanoTime() - start;
            config.addOverhead(overhead);

            getNext().filter(objectContext);
        }

        @Override
        public SyncObject reverseFilter(ObjectContext objectContext) {
            throw new UnsupportedOperationException();
        }
    }

    @FilterConfig(cliName = "no-op")
    public static class NoOpConfig {
        private AtomicLong overhead = new AtomicLong();

        public long getOverhead() {
            return overhead.longValue();
        }

        private void addOverhead(long ns) {
            overhead.addAndGet(ns);
        }
    }
}

/*
 * Copyright (c) 2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync;

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.test.StartNotifyFilter;
import engineering.clientside.throttle.Throttle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// NOTE: throughput rate windows are set to 10 seconds, and any tests shorter than that seem to consistently measure
//       10-20% lower than the throttle rate - so make sure all job durations are longer than 10 seconds.
//       Even when tests are >10 seconds, some read windows are measuring lower, even though the read window unit tests
//       show they should be quite accurate
// TODO: figure out why this happens and correct it
public class ThrottleTest {
    public static final Logger log = LoggerFactory.getLogger(ThrottleTest.class);
    public static final double DELTA_PERCENTAGE = 0.1; //allowed deviation

    // measures duration of execution of a sync job without start-up overhead
    private long time(EcsSync sync) {
        // add start-notify as first filter
        StartNotifyFilter.StartNotifyConfig startNotifyConfig = new StartNotifyFilter.StartNotifyConfig();
        List<Object> filterConfigs = new ArrayList<>();
        filterConfigs.add(startNotifyConfig);
        if (sync.getSyncConfig().getFilters() != null) filterConfigs.addAll(sync.getSyncConfig().getFilters());
        sync.getSyncConfig().setFilters(filterConfigs);

        // run job in background
        CompletableFuture<Void> future = CompletableFuture.runAsync(sync);

        // wait for the job to start (removes start overhead from time measurement)
        try {
            if (!startNotifyConfig.waitForStart(10, TimeUnit.SECONDS))
                Assertions.fail("job took more than 10 seconds to start");
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }

        // time the job
        long startTime = System.currentTimeMillis();
        future.join();
        long endTime = System.currentTimeMillis();

        return endTime - startTime;
    }

    @Test
    public void testJobBandwidthThrottle() throws Exception {
        int bandwidthLimit = 5 * 1024 * 1024; // 5 MiB/s
        int objectSize = 1024 * 1024; // 1 MiB
        int objectCount = 60;

        TestConfig testConfig = new TestConfig().withMinSize(objectSize).withMaxSize(objectSize)
                .withObjectCount(objectCount).withChanceOfChildren(0);
        SyncOptions options = new SyncOptions().withBandwidthLimit(bandwidthLimit);
        SyncConfig syncConfig = new SyncConfig().withOptions(options).withSource(testConfig).withTarget(testConfig);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        long durationMs = time(sync);
        long bwRate = sync.getStats().getBytesComplete() * 1000 / durationMs; // adjust to bytes/second

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(objectCount, sync.getStats().getObjectsComplete());
        log.warn("BW limit: {}, actual rate: {}", bandwidthLimit, bwRate);
        Assertions.assertEquals(bandwidthLimit, bwRate, bandwidthLimit * DELTA_PERCENTAGE);
    }

    @Test
    public void testSharedBandwidthThrottle() throws Exception {
        int bandwidthLimit = 5 * 1024 * 1024; // 5 MiB/s
        int objectSize = 1024 * 1024; // 1 MiB
        int objectCount = 60;

        TestConfig testConfig = new TestConfig().withMinSize(objectSize).withMaxSize(objectSize)
                .withObjectCount(objectCount).withChanceOfChildren(0);
        SyncConfig syncConfig = new SyncConfig().withSource(testConfig).withTarget(testConfig);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSharedBandwidthThrottle(Throttle.create(bandwidthLimit, true));
        long durationMs = time(sync);
        long bwRate = sync.getStats().getBytesComplete() * 1000 / durationMs; // adjust to bytes/second

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(objectCount, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(bandwidthLimit, bwRate, bandwidthLimit * DELTA_PERCENTAGE);
    }

    @Test
    public void testCombinedBandwidthThrottle() throws Exception {
        int lesserBandwidthLimit = 5 * 1024 * 1024; // 5 MiB/s
        int greaterBandwidthLimit = 20 * 1024 * 1024; // 20 MiB/s
        int objectSize = 1024 * 1024; // 1 MiB
        int objectCount = 50;

        // actual bandwidth should always be the lower value
        // first, test with lower value at job-level
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig()
                .withSource(new TestConfig().withMinSize(objectSize).withMaxSize(objectSize)
                        .withObjectCount(objectCount).withChanceOfChildren(0))
                .withTarget(new TestConfig())
                // lower value at job-level
                .withOptions(new SyncOptions().withBandwidthLimit(lesserBandwidthLimit)));
        // higher value at shared level
        sync.setSharedBandwidthThrottle(Throttle.create(greaterBandwidthLimit, true));
        long durationMs = time(sync);
        long bwRate = sync.getStats().getBytesComplete() * 1000 / durationMs; // adjust to bytes/second

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(objectCount, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(lesserBandwidthLimit, bwRate, lesserBandwidthLimit * DELTA_PERCENTAGE);

        // next, test with lower value at shared level
        // first, change the lower limit, to minimize confusion between tests
        lesserBandwidthLimit = 10 * 1024 * 1024; // 10 MiB/s

        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig()
                .withSource(new TestConfig().withMinSize(objectSize).withMaxSize(objectSize)
                        .withObjectCount(objectCount).withChanceOfChildren(0))
                .withTarget(new TestConfig())
                // higher value at job-level
                .withOptions(new SyncOptions().withBandwidthLimit(greaterBandwidthLimit)));
        // lower value at shared level
        sync.setSharedBandwidthThrottle(Throttle.create(lesserBandwidthLimit, true));
        durationMs = time(sync);
        bwRate = sync.getStats().getBytesComplete() * 1000 / durationMs; // adjust to bytes/second

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(objectCount, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(lesserBandwidthLimit, bwRate, lesserBandwidthLimit * DELTA_PERCENTAGE);
    }

    @Test
    public void testMultipleSharedBandwidthThrottle() throws Exception {
        int bandwidthLimit = 20 * 1024 * 1024; // 20 MiB/s
        int objectSize = 1024 * 1024; // 1 MiB
        int jobObjectCount = 60;

        Throttle sharedBandwidthThrottle = Throttle.create(bandwidthLimit, true);
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        EcsSync sync, sync2, sync3;

        try {
            sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig()
                    .withSource(new TestConfig().withMinSize(objectSize).withMaxSize(objectSize)
                            .withObjectCount(jobObjectCount).withChanceOfChildren(0))
                    .withTarget(new TestConfig()));
            sync.setSharedBandwidthThrottle(sharedBandwidthThrottle);
            futures.add(CompletableFuture.supplyAsync(() -> time(sync), executor));

            sync2 = new EcsSync();
            sync2.setSyncConfig(new SyncConfig()
                    .withSource(new TestConfig().withMinSize(objectSize).withMaxSize(objectSize)
                            .withObjectCount(jobObjectCount).withChanceOfChildren(0))
                    .withTarget(new TestConfig()));
            sync2.setSharedBandwidthThrottle(sharedBandwidthThrottle);
            futures.add(CompletableFuture.supplyAsync(() -> time(sync2), executor));

            sync3 = new EcsSync();
            sync3.setSyncConfig(new SyncConfig()
                    .withSource(new TestConfig().withMinSize(objectSize).withMaxSize(objectSize)
                            .withObjectCount(jobObjectCount).withChanceOfChildren(0))
                    .withTarget(new TestConfig()));
            sync3.setSharedBandwidthThrottle(sharedBandwidthThrottle);
            futures.add(CompletableFuture.supplyAsync(() -> time(sync3), executor));
        } finally {
            executor.shutdown(); // all submitted tasks will finish
        }

        for (CompletableFuture<?> future : futures) {
            future.join();
        }

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(0, sync2.getStats().getObjectsFailed());
        Assertions.assertEquals(0, sync3.getStats().getObjectsFailed());
        Assertions.assertEquals(jobObjectCount, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(jobObjectCount, sync2.getStats().getObjectsComplete());
        Assertions.assertEquals(jobObjectCount, sync3.getStats().getObjectsComplete());

        // total throughput for all jobs should match the shared throttle
        long duration1 = futures.get(0).get(), duration2 = futures.get(1).get(), duration3 = futures.get(2).get();
        long bwRate1 = sync.getStats().getBytesComplete() * 1000 / duration1; // adjust to bytes/second
        long bwRate2 = sync2.getStats().getBytesComplete() * 1000 / duration2; // adjust to bytes/second
        long bwRate3 = sync3.getStats().getBytesComplete() * 1000 / duration3; // adjust to bytes/second
        long totalReadRate = bwRate1 + bwRate2 + bwRate3;
        log.warn("Bandwidth limit (total): {}, actual rate: {}", bandwidthLimit, totalReadRate);
        Assertions.assertEquals(bandwidthLimit, totalReadRate, bandwidthLimit * DELTA_PERCENTAGE);

        // each job has the same thread count, so should have an equal effective bandwidth
        log.warn("Bandwidth limit (1): {}, actual rate: {}", bandwidthLimit / 3.0, bwRate1);
        Assertions.assertEquals(bandwidthLimit / 3.0, bwRate1, bandwidthLimit * DELTA_PERCENTAGE / 3);
        log.warn("Bandwidth limit (2): {}, actual rate: {}", bandwidthLimit / 3.0, bwRate2);
        Assertions.assertEquals(bandwidthLimit / 3.0, bwRate2, bandwidthLimit * DELTA_PERCENTAGE / 3);
        log.warn("Bandwidth limit (3): {}, actual rate: {}", bandwidthLimit / 3.0, bwRate3);
        Assertions.assertEquals(bandwidthLimit / 3.0, bwRate3, bandwidthLimit * DELTA_PERCENTAGE / 3);
    }

    @Test
    public void testJobTpsThrottle() {
        int tpsLimit = 10; // objects/s
        int objectCount = 120;

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig()
                .withSource(new TestConfig().withMaxSize(0).withChanceOfChildren(0).withObjectCount(objectCount))
                .withTarget(new TestConfig())
                .withOptions(new SyncOptions().withThroughputLimit(tpsLimit)));
        long durationMs = time(sync);
        float tpsRate = (float) sync.getStats().getObjectsComplete() * 1000 / durationMs; // adjust to objects/second

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(objectCount, sync.getStats().getObjectsComplete());
        log.warn("TPS limit: {}, actual rate: {}", tpsLimit, tpsRate);
        Assertions.assertEquals((float) tpsLimit, tpsRate, (float) tpsLimit * DELTA_PERCENTAGE);
    }

    @Test
    public void testSharedTpsThrottle() {
        int tpsLimit = 10; // objects/second
        int objectCount = 120;

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig()
                .withSource(new TestConfig().withMaxSize(0).withChanceOfChildren(0).withObjectCount(objectCount))
                .withTarget(new TestConfig()));
        sync.setSharedThroughputThrottle(Throttle.create(tpsLimit, true));
        long durationMs = time(sync);
        float tpsRate = (float) sync.getStats().getObjectsComplete() * 1000 / durationMs; // adjust to objects/second

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(objectCount, sync.getStats().getObjectsComplete());
        log.warn("TPS limit: {}, actual rate: {}", tpsLimit, tpsRate);
        Assertions.assertEquals((float) tpsLimit, tpsRate, (float) tpsLimit * DELTA_PERCENTAGE);
    }

    @Test
    public void testCombinedTpsThrottle() {
        int lesserTpsLimit = 10; // objects/second
        int greaterTpsLimit = 30; // objects/second
        int objectCount = 100;

        // actual TPS should always be the lower value
        // first, test with lower value at job-level
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig()
                .withSource(new TestConfig().withMaxSize(0).withChanceOfChildren(0).withObjectCount(objectCount))
                .withTarget(new TestConfig())
                // lower value at job-level
                .withOptions(new SyncOptions().withThroughputLimit(lesserTpsLimit)));
        // higher value at shared level
        sync.setSharedThroughputThrottle(Throttle.create(greaterTpsLimit, true));
        long durationMs = time(sync);
        float tpsRate = (float) sync.getStats().getObjectsComplete() * 1000 / durationMs; // adjust to objects/second

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(objectCount, sync.getStats().getObjectsComplete());
        log.warn("TPS limit: {}, actual rate: {}", lesserTpsLimit, tpsRate);
        Assertions.assertEquals((float) lesserTpsLimit, tpsRate, (float) lesserTpsLimit * DELTA_PERCENTAGE);

        // next, test with lower value at shared level
        // first, change the lower limit, to minimize confusion between tests
        lesserTpsLimit = 20;

        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig()
                .withSource(new TestConfig().withMaxSize(0).withChanceOfChildren(0).withObjectCount(objectCount))
                .withTarget(new TestConfig())
                // higher value at job-level
                .withOptions(new SyncOptions().withThroughputLimit(greaterTpsLimit)));
        // lower value at shared level
        sync.setSharedThroughputThrottle(Throttle.create(lesserTpsLimit, true));
        durationMs = time(sync);
        tpsRate = (float) sync.getStats().getObjectsComplete() * 1000 / durationMs; // adjust to objects/second

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(objectCount, sync.getStats().getObjectsComplete());
        log.warn("TPS limit: {}, actual rate: {}", lesserTpsLimit, sync.getStats().getObjectCompleteRate());
        Assertions.assertEquals((float) lesserTpsLimit, sync.getStats().getObjectCompleteRate(), (float) lesserTpsLimit * DELTA_PERCENTAGE);
    }

    // tests aggregate throttle across jobs and fairness between threads
    @Test
    public void testMultipleSharedTpsThrottle() throws Exception {
        int tpsLimit = 30; // objects/second
        int jobObjectCount = 100;

        Throttle sharedTpsThrottle = Throttle.create(tpsLimit, true);
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        EcsSync sync, sync2, sync3;

        try {
            sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig()
                    .withSource(new TestConfig().withMaxSize(0).withChanceOfChildren(0).withObjectCount(jobObjectCount))
                    .withTarget(new TestConfig()));
            sync.setSharedThroughputThrottle(sharedTpsThrottle);
            futures.add(CompletableFuture.supplyAsync(() -> time(sync), executor));

            sync2 = new EcsSync();
            sync2.setSyncConfig(new SyncConfig()
                    .withSource(new TestConfig().withMaxSize(0).withChanceOfChildren(0).withObjectCount(jobObjectCount))
                    .withTarget(new TestConfig()));
            sync2.setSharedThroughputThrottle(sharedTpsThrottle);
            futures.add(CompletableFuture.supplyAsync(() -> time(sync2), executor));

            sync3 = new EcsSync();
            sync3.setSyncConfig(new SyncConfig()
                    .withSource(new TestConfig().withMaxSize(0).withChanceOfChildren(0).withObjectCount(jobObjectCount))
                    .withTarget(new TestConfig()));
            sync3.setSharedThroughputThrottle(sharedTpsThrottle);
            futures.add(CompletableFuture.supplyAsync(() -> time(sync3), executor));
        } finally {
            executor.shutdown(); // all submitted tasks will finish
        }

        for (CompletableFuture<?> future : futures) {
            future.join();
        }

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(0, sync2.getStats().getObjectsFailed());
        Assertions.assertEquals(0, sync3.getStats().getObjectsFailed());
        Assertions.assertEquals(jobObjectCount, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(jobObjectCount, sync2.getStats().getObjectsComplete());
        Assertions.assertEquals(jobObjectCount, sync3.getStats().getObjectsComplete());

        long duration1 = futures.get(0).get(), duration2 = futures.get(1).get(), duration3 = futures.get(2).get();
        float tpsRate1 = (float) sync.getStats().getObjectsComplete() * 1000 / duration1; // adjust to objects/second
        float tpsRate2 = (float) sync2.getStats().getObjectsComplete() * 1000 / duration2; // adjust to objects/second
        float tpsRate3 = (float) sync3.getStats().getObjectsComplete() * 1000 / duration3; // adjust to objects/second

        // total throughput for all jobs should match the shared throttle
        float totalCompletionRate = tpsRate1 + tpsRate2 + tpsRate3;
        log.warn("TPS limit (total): {}, actual rate: {}", tpsLimit, totalCompletionRate);
        Assertions.assertEquals((float) tpsLimit, totalCompletionRate, (float) tpsLimit * DELTA_PERCENTAGE);

        // each job has the same thread count, so should have an equal effective throughput
        log.warn("TPS limit (1): {}, actual rate: {}", tpsLimit / 3.0, tpsRate1);
        Assertions.assertEquals((float) tpsLimit / 3.0, tpsRate1, (float) tpsLimit * DELTA_PERCENTAGE / 3);
        log.warn("TPS limit (2): {}, actual rate: {}", tpsLimit / 3.0, tpsRate2);
        Assertions.assertEquals((float) tpsLimit / 3.0, tpsRate2, (float) tpsLimit * DELTA_PERCENTAGE / 3);
        log.warn("TPS limit (3): {}, actual rate: {}", tpsLimit / 3.0, tpsRate3);
        Assertions.assertEquals((float) tpsLimit / 3.0, tpsRate3, (float) tpsLimit * DELTA_PERCENTAGE / 3);
    }
}

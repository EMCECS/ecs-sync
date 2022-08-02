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
package com.emc.ecs.sync.util;

import engineering.clientside.throttle.Throttle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ThrottledInputStreamTest {
    private static final double FIRST_DELTA = 0.007; // 7ms
    private static final double SECOND_DELTA = 0.006; // 6ms
    private static final double DELTA_PERCENTAGE = 0.05; // 5%
    private static final ThreadLocalRandom random = ThreadLocalRandom.current();

    @Test
    public void testAcquireWeights() throws InterruptedException {
        final Throttle throttle = Throttle.create(20.0);
        Assertions.assertEquals(0.00, throttle.acquireUnchecked(1), FIRST_DELTA);
        Assertions.assertEquals(0.05, throttle.acquire(1), SECOND_DELTA);
        Assertions.assertEquals(0.05, throttle.acquire(2), SECOND_DELTA);
        Assertions.assertEquals(0.10, throttle.acquire(4), SECOND_DELTA);
        Assertions.assertEquals(0.20, throttle.acquire(8), SECOND_DELTA);
        Assertions.assertEquals(0.40, throttle.acquire(1), SECOND_DELTA);
    }

    @Test
    public void testAcquireDelays() {
        Throttle throttle = Throttle.create(20);
        // all of these calls should return immediately, so all the delays should be based on the same start time
        // acquireDelayDuration returns nanoseconds, so we need to convert to milliseconds
        // NOTE: this test reveals details about the algorithm Throttle uses to determine wait times.
        //       I.e. it introduces waits *after* acquisition, so if you acquire 1 permit, or 1000 permits, you wait the
        //       same amount of time, but the next call to acquire will wait for your permits to be refreshed
        Assertions.assertEquals(0, throttle.acquireDelayDuration(1) / 1_000_000);
        Assertions.assertEquals(50, throttle.acquireDelayDuration(1) / 1_000_000, 5);
        Assertions.assertEquals(100, throttle.acquireDelayDuration(2) / 1_000_000, 5);
        Assertions.assertEquals(200, throttle.acquireDelayDuration(4) / 1_000_000, 5);
        Assertions.assertEquals(400, throttle.acquireDelayDuration(8) / 1_000_000, 5);
        Assertions.assertEquals(800, throttle.acquireDelayDuration(1) / 1_000_000, 5);
        Assertions.assertEquals(850, throttle.acquireDelayDuration(100) / 1_000_000, 5);
    }

    @Test
    public void testSingleThreadThrottle() {
        long permitsPerSecond = 1024 * 1024; // 1MiB/s
        long size = 10 * 1024 * 1024; // 10MiB
        final Throttle throttle = Throttle.create(permitsPerSecond);

        ThrottledInputStream throttledInputStream = new ThrottledInputStream(new RandomInputStream(size), throttle);

        long nowMillisStart = System.currentTimeMillis();
        long totalRead = SyncUtil.consumeAndCloseStream(throttledInputStream);
        long nowMillisEnd = System.currentTimeMillis();

        Assertions.assertEquals(size, totalRead);
        double expectedTime = (double) size / permitsPerSecond;
        double actualTime = (nowMillisEnd - nowMillisStart) / 1000.0;
        Assertions.assertEquals(expectedTime, actualTime, expectedTime * DELTA_PERCENTAGE);
    }

    @Test
    public void testParallelThreadThrottle() throws ExecutionException, InterruptedException {
        long permitsPerSecond = 5 * 1024 * 1024; // 5 MiB/s
        int randomDataSize = 2 * 1024 * 1024; // 2 MiB
        int threads = 20;

        final Throttle throttle = Throttle.create(permitsPerSecond);
        List<Future<?>> futures = new ArrayList<>();
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final AtomicLong overallRead = new AtomicLong();
        List<Double> resultList = Collections.synchronizedList(new ArrayList<>());
        byte[] data = new byte[randomDataSize];
        random.nextBytes(data);

        long nowMillisStart = System.currentTimeMillis();
        for (int i = 0; i < threads; i++) {
            int threadNo = i;
            futures.add(executor.submit(() -> {
                try (ThrottledInputStream throttledInputStream = new ThrottledInputStream(new ByteArrayInputStream(data), throttle)) {
                    long millisStart = System.currentTimeMillis();
                    long totalRead = SyncUtil.consumeAndCloseStream(throttledInputStream);
                    long millisEnd = System.currentTimeMillis();
                    overallRead.getAndAdd(totalRead);

                    Assertions.assertEquals(randomDataSize, totalRead);
                    resultList.add((millisEnd - millisStart) / 1000.0);
                    System.out.println("Thread#" + threadNo + ": read size:" + totalRead + ", time spent: " + (millisEnd - nowMillisStart) / 1000.0 + "s.");
                } catch (IOException e) {
                    Assertions.fail();
                }
            }));
        }
        for (Future<?> future : futures) {
            future.get();
        }
        long nowMillisEnd = System.currentTimeMillis();
        executor.shutdown();

        Assertions.assertEquals(randomDataSize * threads, overallRead.get());
        double averageTime = resultList.stream().mapToDouble(d -> d).average().getAsDouble();
        for (double item : resultList) {
            Assertions.assertEquals(averageTime, item, averageTime * DELTA_PERCENTAGE);
        }
        double expectedTime = randomDataSize * threads / permitsPerSecond;
        Assertions.assertEquals(expectedTime, (nowMillisEnd - nowMillisStart) / 1000.0, expectedTime * DELTA_PERCENTAGE);
    }
}
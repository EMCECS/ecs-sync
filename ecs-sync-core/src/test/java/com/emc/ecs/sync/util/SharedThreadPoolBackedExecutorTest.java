/*
 * Copyright (c) 2013-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class SharedThreadPoolBackedExecutorTest {
    @Test
    public void testShutdownNow() throws InterruptedException {
        EnhancedThreadPoolExecutor sharedExecutor = new EnhancedThreadPoolExecutor(10, new LinkedBlockingDeque<>(), "shared-pool");
        try {
            sharedExecutor.prestartAllCoreThreads();
            ExecutorService executor1 = new SharedThreadPoolBackedExecutor(sharedExecutor);
            ExecutorService executor2 = new SharedThreadPoolBackedExecutor(sharedExecutor);

            // alternate submissions - 100 per executor
            for (int i = 0; i < 100; i++) {
                executor1.submit(this::waitATick);
                executor2.submit(this::waitAndGive);
            }

            // wait just over 5 seconds - should run 50 tasks
            Thread.sleep(5100);

            // shutdown both executors
            List<Runnable> runList1 = executor1.shutdownNow();
            List<Runnable> runList2 = executor2.shutdownNow();
            // give some time for tasks to interrupt and flush
            Thread.sleep(100);

            // at this point, all queued tasks will become no-ops in the underlying pool and should execute immediately
            // all active tasks will be interrupted and should not increment the counters
            Assertions.assertEquals(0, runList1.size());
            Assertions.assertEquals(0, runList2.size());
            // there should be no active threads in the shared pool
            Assertions.assertEquals(0, sharedExecutor.getActiveCount());
            // because the submissions were alternated, the counters should be even
            Assertions.assertEquals(25, counter1.get());
            Assertions.assertEquals(25, counter2.get());
        } finally {
            sharedExecutor.shutdownNow();
        }
    }

    @Test
    public void testIsolation() throws InterruptedException {
        int threadCount = 8;
        EnhancedThreadPoolExecutor sharedExecutor = new EnhancedThreadPoolExecutor(threadCount, new LinkedBlockingDeque<>(), "shared-pool");
        try {
            sharedExecutor.prestartAllCoreThreads();
            ExecutorService executor1 = new SharedThreadPoolBackedExecutor(sharedExecutor);
            ExecutorService executor2 = new SharedThreadPoolBackedExecutor(sharedExecutor);

            // alternate submissions - 100 per executor
            for (int i = 0; i < 100; i++) {
                executor1.submit(this::waitATick);
                executor2.submit(this::waitAndGive);
            }

            // wait just over 5 seconds - should run 5 x threadCount tasks
            Thread.sleep(5100);

            // shutdown 1st executor
            List<Runnable> runList1 = executor1.shutdownNow();
            // give some time for tasks to interrupt and flush
            Thread.sleep(100);

            // at this point, executor1 tasks should flush out of the active threads, leaving only executor2 tasks
            Assertions.assertEquals(0, runList1.size());
            // but all threads should be active (running executor2 tasks)
            Assertions.assertEquals(threadCount, sharedExecutor.getActiveCount());
            // because the submissions were alternated, the counters should be even
            Assertions.assertEquals(5 * threadCount / 2, counter1.get());
            Assertions.assertEquals(5 * threadCount / 2, counter2.get());

            // wait just over 2 more seconds - should run 2 x threadCount tasks
            Thread.sleep(2100);

            // shut down 2nd executor
            List<Runnable> runList2 = executor2.shutdownNow();
            // give some time for tasks to interrupt and flush
            Thread.sleep(100);

            // all tasks should flush out
            Assertions.assertEquals(0, runList2.size());
            // no threads should be active
            Assertions.assertEquals(0, sharedExecutor.getActiveCount());
            // counter1 should not have changed
            Assertions.assertEquals(5 * threadCount / 2, counter1.get());
            // counter2 should be incremented 2 x threadCount times
            Assertions.assertEquals(9 * threadCount / 2, counter2.get());
        } finally {
            sharedExecutor.shutdownNow();
        }
    }

    @Test
    public void testCompletableFuture() {
        EnhancedThreadPoolExecutor sharedExecutor = new EnhancedThreadPoolExecutor(4, new LinkedBlockingDeque<>(), "shared-pool");
        try {
            sharedExecutor.prestartAllCoreThreads();
            ExecutorService executor = new SharedThreadPoolBackedExecutor(sharedExecutor);

            CompletableFuture.supplyAsync(() -> {
                sleep(1000);
                return true;
            }, executor);
            CompletableFuture.runAsync(() -> sleep(1000), executor);
            CompletableFuture.runAsync(() -> {
                throw new RuntimeException("foo");
            }, executor).exceptionally(throwable -> {
                sleep(1000);
                return null;
            });
        } finally {
            sharedExecutor.shutdownNow();
        }
    }

    private final AtomicInteger counter1 = new AtomicInteger();
    private final AtomicInteger counter2 = new AtomicInteger();

    public void waitATick() {
        try {
            Thread.sleep(1000);
            counter1.incrementAndGet();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public int waitAndGive() throws InterruptedException {
        Thread.sleep(1000);
        return counter2.incrementAndGet();
    }

    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

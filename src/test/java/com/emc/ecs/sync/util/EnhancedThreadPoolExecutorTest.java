/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class EnhancedThreadPoolExecutorTest {
    private static Logger log = LoggerFactory.getLogger(EnhancedThreadPoolExecutorTest.class);

    public static final int TASK_TIME = 1500; // ms
    public static final int INTERVAL_TIME = 1200; // ms
    public static final int TASK_COUNT = 40;
    public static final int[] THREAD_COUNT = {8, 12, 2, 8};

    @Test
    public void testImmediateResizing() throws Exception {
        // timing may be an issue here, so we'll use 3 rounds of 1000ms jobs and 700ms pause intervals to give us a
        // buffer of execution time between threads.. this way, local execution time can be between 0 and 300ms
        long start = System.currentTimeMillis();

        // total jobs
        BlockingDeque<Runnable> workDeque = new LinkedBlockingDeque<>();
        for (int i = 0; i < TASK_COUNT; i++) {
            workDeque.add(new LazyJob());
        }

        // first interval
        EnhancedThreadPoolExecutor executor = new EnhancedThreadPoolExecutor(THREAD_COUNT[0], workDeque);
        Assert.assertEquals(THREAD_COUNT[0], executor.prestartAllCoreThreads());

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // verify jobs are removed and active threads is correct
        log.warn("tim: {}, active count: {}", System.currentTimeMillis() - start, executor.getActiveCount());
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0], workDeque.size());
        Assert.assertTrue(Math.abs(executor.getActiveCount() - THREAD_COUNT[0]) < 1);

        // resize pool
        executor.resizeThreadPool(THREAD_COUNT[1]);

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // verify jobs are removed and active threads is correct
        log.warn("tim: {}, active count: {}", System.currentTimeMillis() - start, executor.getActiveCount());
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0] - THREAD_COUNT[1], workDeque.size());
        Assert.assertTrue(Math.abs(executor.getActiveCount() - THREAD_COUNT[1]) < 1);

        // resize pool
        executor.resizeThreadPool(THREAD_COUNT[2]);

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // verify jobs are removed and active threads is correct
        log.warn("tim: {}, active count: {}", System.currentTimeMillis() - start, executor.getActiveCount());
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0] - THREAD_COUNT[1] - THREAD_COUNT[2], workDeque.size());
        Assert.assertTrue(Math.abs(executor.getActiveCount() - THREAD_COUNT[2]) < 1);

        // resize pool
        executor.resizeThreadPool(THREAD_COUNT[3]);

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // verify jobs are removed and active threads is correct
        log.warn("tim: {}, active count: {}", System.currentTimeMillis() - start, executor.getActiveCount());
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0] - THREAD_COUNT[1] - THREAD_COUNT[2] - THREAD_COUNT[3], workDeque.size());
        Assert.assertTrue(Math.abs(executor.getActiveCount() - THREAD_COUNT[3]) < 1);

        executor.shutdown();
    }

    @Test
    public void testPauseResume() throws Exception {
        // total jobs
        BlockingDeque<Runnable> workDeque = new LinkedBlockingDeque<>();
        for (int i = 0; i < TASK_COUNT; i++) {
            workDeque.add(new LazyJob());
        }

        // first interval
        EnhancedThreadPoolExecutor executor = new EnhancedThreadPoolExecutor(THREAD_COUNT[0], workDeque);
        Assert.assertEquals(THREAD_COUNT[0], executor.prestartAllCoreThreads());

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // verify jobs are removed and active threads is correct
        log.warn("active count: {}", executor.getActiveCount());
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0], workDeque.size());

        // pause
        Assert.assertTrue(executor.pause());

        // 2nd call should return false, but is idempotent otherwise
        Assert.assertFalse(executor.pause());

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // one round of jobs should be pulled from the queue (they are all waiting to be executed though)
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0] - THREAD_COUNT[0], workDeque.size());

        // resume
        executor.resume();

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // no new jobs should have been pulled from the queue (they were waiting to be executed last interval)
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0] - THREAD_COUNT[0], workDeque.size());

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // one round of jobs should have been pulled from the queue
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0] - THREAD_COUNT[0] - THREAD_COUNT[0], workDeque.size());

        executor.shutdown();
    }

    @Test
    public void testPauseThenStop() throws Exception {
        int jobCount = 20, threadCount = 4;
        // create executor and submit jobs
        BlockingDeque<Runnable> workDeque = new LinkedBlockingDeque<>();
        EnhancedThreadPoolExecutor executor = new EnhancedThreadPoolExecutor(threadCount, workDeque);
        for (int i = 0; i < jobCount; i++) {
            executor.submit(new LazyJob());
        }

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // verify jobs are removed and active threads is correct
        Assert.assertEquals(jobCount - threadCount, workDeque.size());
        Assert.assertEquals(threadCount, executor.getActiveCount());

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // two rounds of jobs should be complete now
        Assert.assertEquals(jobCount - threadCount - threadCount, workDeque.size());
        Assert.assertEquals(threadCount, executor.getActiveCount());

        // pause
        Assert.assertTrue(executor.pause());

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // active threads should be 0 and 3 rounds of jobs are complete
        Assert.assertEquals(jobCount - (threadCount * 3), workDeque.size());
        Assert.assertEquals(0, executor.getActiveCount());
        Assert.assertTrue(executor.isPaused());

        // stop (gracefully)
        executor.stop();

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // total completed jobs should not have changed and there should be no unfinished tasks
        Assert.assertTrue(executor.isTerminated());
        Assert.assertEquals(threadCount * 3, executor.getCompletedTaskCount());
        Assert.assertEquals(0, executor.getActiveCount());
    }

    static class LazyJob implements Runnable {
        @Override
        public void run() {
            try {
                Thread.sleep(TASK_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

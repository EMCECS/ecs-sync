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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class EnhancedThreadPoolExecutorTest {
    public static final int TASK_TIME = 1000; // ms
    public static final int INTERVAL_TIME = 700; // ms
    public static final int TASK_COUNT = 30;
    public static final int[] THREAD_COUNT = {8, 12, 4};

    @Test
    public void testImmediateResizing() throws Exception {
        // timing may be an issue here, so we'll use 3 rounds of 1000ms jobs and 700ms pause intervals to give us a
        // buffer of execution time between threads.. this way, local execution time can be between 0 and 300ms

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
        System.out.println("active count: " + executor.getActiveCount());
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0], workDeque.size());
        Assert.assertTrue(Math.abs(executor.getActiveCount() - THREAD_COUNT[0]) < 1);

        // resize pool
        executor.resizeThreadPool(THREAD_COUNT[1]);

        // should not take effect yet
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0], workDeque.size());

        // this should create new threads
        executor.prestartAllCoreThreads();

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // verify jobs are removed and active threads is correct
        System.out.println("active count: " + executor.getActiveCount());
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0] - THREAD_COUNT[1], workDeque.size());
        Assert.assertTrue(Math.abs(executor.getActiveCount() - THREAD_COUNT[1]) < 1);

        // resize pool
        executor.resizeThreadPool(THREAD_COUNT[2]);

        // should not take effect until next task is complete
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0] - THREAD_COUNT[1], workDeque.size());

        // wait interval
        Thread.sleep(INTERVAL_TIME);

        // verify jobs are removed and active threads is correct
        System.out.println("active count: " + executor.getActiveCount());
        Assert.assertEquals(TASK_COUNT - THREAD_COUNT[0] - THREAD_COUNT[1] - THREAD_COUNT[2], workDeque.size());
        Assert.assertTrue(Math.abs(executor.getActiveCount() - THREAD_COUNT[2]) < 1);

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
        System.out.println("active count: " + executor.getActiveCount());
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

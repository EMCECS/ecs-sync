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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EnhancedThreadPoolExecutor extends ThreadPoolExecutor {
    private static final Logger log = LoggerFactory.getLogger(EnhancedThreadPoolExecutor.class);

    public static final String DEFAULT_POOL_NAME = "x-pool";

    private BlockingDeque<Runnable> workDeque;
    private boolean shutdownWhenIdle = false;
    private Semaphore threadsToKill = new Semaphore(0);
    private final Object pauseLock = new Object();
    private boolean paused = false;
    private final Object submitLock = new Object();
    private AtomicLong unfinishedTasks = new AtomicLong();
    private AtomicInteger activeTasks = new AtomicInteger();

    public EnhancedThreadPoolExecutor(int poolSize, BlockingDeque<Runnable> workDeque) {
        this(poolSize, workDeque, DEFAULT_POOL_NAME);
    }

    public EnhancedThreadPoolExecutor(int poolSize, BlockingDeque<Runnable> workDeque, String poolName) {
        this(poolSize, workDeque, new NamedThreadFactory(poolName));
    }

    public EnhancedThreadPoolExecutor(int poolSize, BlockingDeque<Runnable> workDeque, ThreadFactory threadFactory) {
        super(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, workDeque, threadFactory);
        this.workDeque = workDeque;
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        if (threadsToKill.tryAcquire()) {
            log.debug("terminating thread due to shrinking pool");

            // we need a Deque here so the job order isn't disturbed
            workDeque.addFirst(r);

            // throwing an exception is the only way to immediately kill a thread in the pool. otherwise, the entire
            // work queue must be empty before the pool will start to shrink (see ExceptionHandler class below)
            throw new PoolTooLargeException("killing thread to shrink pool");
        }

        // a new task started, so the queue should be smaller.
        synchronized (submitLock) {
            submitLock.notify();
        }

        synchronized (pauseLock) {
            if (paused) {
                log.debug("thread has been paused");
                try {
                    pauseLock.wait();
                    log.debug("thread has been resumed");
                } catch (InterruptedException e) {
                    log.warn("interrupted while paused; might be shutting down");
                    workDeque.addFirst(r);
                    throw new RuntimeException(e);
                }
            }
        }

        activeTasks.incrementAndGet();

        super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        long aTasks = activeTasks.decrementAndGet();
        long uTasks = unfinishedTasks.decrementAndGet();

        // triple-check to make sure we don't shutdown too soon
        if (shutdownWhenIdle && aTasks == 0 && uTasks == 0 && workDeque.isEmpty()) {
            log.info("shutting down pool because it is idle and shutdownWhenIdle is true");
            shutdown();
        }

        super.afterExecute(r, t);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new EnhancedFutureTask<>(runnable, value);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new EnhancedFutureTask<>(callable);
    }

    /**
     * This will attempt to submit the task to the pool and, in the case where the queue is full, block until space is
     * available
     *
     * @throws IllegalStateException if the executor is shutting down or terminated
     */
    public void blockingSubmit(Runnable task) {
        while (true) {
            if (this.isShutdown()) throw new IllegalStateException("executor is shut down");

            synchronized (submitLock) {
                try {
                    this.submit(task);
                    return;
                } catch (RejectedExecutionException e) {
                    // ignore
                }
                if (this.isShutdown()) throw new IllegalStateException("executor is shut down");
                try {
                    log.debug("task queue is full; waiting until space is available");
                    submitLock.wait();
                    log.debug("task queue has space; resubmitting task");
                } catch (InterruptedException e) {
                    log.warn("interrupted while waiting to submit a task", e);
                }
            }
        }
    }

    @Override
    @Nonnull
    public Future<?> submit(Runnable task) {
        Future<?> future = super.submit(task);
        unfinishedTasks.incrementAndGet();
        return future;
    }

    @Override
    @Nonnull
    public <T> Future<T> submit(Runnable task, T result) {
        Future<T> future = super.submit(task, result);
        unfinishedTasks.incrementAndGet();
        return future;
    }

    @Override
    @Nonnull
    public <T> Future<T> submit(Callable<T> task) {
        Future<T> future = super.submit(task);
        unfinishedTasks.incrementAndGet();
        return future;
    }

    /**
     * This will attempt to submit the task to the pool and, in the case where the queue is full, block until space is
     * available
     *
     * @throws IllegalStateException if the executor is shutting down or terminated
     */
    public <T> Future<T> blockingSubmit(Callable<T> task) {
        FutureTask<T> futureTask = new FutureTask<>(task);
        blockingSubmit(futureTask);
        return futureTask;
    }

    /**
     * This will set both the core and max pool size and kill any excess threads as their tasks complete.
     */
    public void resizeThreadPool(int newPoolSize) {

        // negate any last resize attempts
        threadsToKill.drainPermits();
        int diff = getActiveCount() - newPoolSize;
        super.setCorePoolSize(newPoolSize);
        super.setMaximumPoolSize(newPoolSize);
        if (diff > 0) threadsToKill.release(diff);
    }

    /**
     * If possible, pauses the executor so that active threads will complete their current task and then wait to execute
     * new tasks from the queue until unpaused.
     *
     * @return true if the state of the executor was changed from running to paused, false if already paused
     * @throws IllegalStateException if the executor is shutting down or terminated
     */
    public boolean pause() {
        synchronized (pauseLock) {
            if (isShutdown()) throw new IllegalStateException("executor is shut down");
            boolean wasPaused = paused;
            paused = true;
            return !wasPaused;
        }
    }

    /**
     * If possible, resumes the executor so that tasks will continue to be executed from the queue.
     *
     * @return true if the state of the executor was changed from paused to running, false if already running
     * @throws IllegalStateException if the executor is shutting down or terminated
     * @see #pause()
     */
    public boolean resume() {
        synchronized (pauseLock) {
            if (isShutdown()) throw new IllegalStateException("executor is shut down");
            boolean wasPaused = paused;
            paused = false;
            pauseLock.notifyAll();
            return wasPaused;
        }
    }

    @Override
    public int getActiveCount() {
        return activeTasks.get();
    }

    public long getUnfinishedTasks() {
        return unfinishedTasks.get();
    }

    public boolean isShutdownWhenIdle() {
        return shutdownWhenIdle;
    }

    /**
     * When true, the next time this executor is idle (the task queue is empty and there are no active tasks), it will
     * shut itself down.
     * <p/>
     * NOTE: only set this to true if you are sure the pool will not go idle until all tasks have been submitted.
     */
    public void setShutdownWhenIdle(boolean shutdownWhenIdle) {
        this.shutdownWhenIdle = shutdownWhenIdle;
    }

    static class PoolTooLargeException extends RuntimeException {
        public PoolTooLargeException(String message) {
            super(message);
        }
    }

    static class ExceptionHandler implements Thread.UncaughtExceptionHandler {
        private Thread.UncaughtExceptionHandler handler;

        public ExceptionHandler(Thread.UncaughtExceptionHandler handler) {
            this.handler = handler;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (!(e instanceof PoolTooLargeException)) {
                log.warn("uncaught exception from task", e);
                if (handler != null) handler.uncaughtException(t, e);
            }
        }
    }

    static class NamedThreadFactory implements ThreadFactory {
        private static AtomicInteger poolNumber = new AtomicInteger();
        private AtomicInteger threadNumber = new AtomicInteger();
        private String threadPrefix;

        public NamedThreadFactory(String poolName) {
            if (poolName == null) {
                this.threadPrefix = DEFAULT_POOL_NAME + "-" + poolNumber.incrementAndGet() + "-t-";
            } else {
                this.threadPrefix = poolName + "-t-";
            }
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, threadPrefix + threadNumber.incrementAndGet());
            t.setUncaughtExceptionHandler(new ExceptionHandler(t.getUncaughtExceptionHandler()));
            return t;
        }
    }
}

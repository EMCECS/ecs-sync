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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Intended to provide isolated, generic ExecutorService functionality, but backed by an
 * {@link EnhancedThreadPoolExecutor} instance that is shared with other logic. <code>submit()</code> methods here
 * will delegate to <code>blockingSubmit()</code> methods in {@link EnhancedThreadPoolExecutor}.
 * {@link #shutdown()} will not actually shut down the shared thread pool, but will functionally disallow additional
 * submits to this wrapper. Similarly, {@link #shutdownNow()} and {@link #awaitTermination(long, TimeUnit)}
 * will consider only the tasks submitted through this wrapper, and are unaware of other tasks in the underlying
 * thread pool.
 */
public class SharedThreadPoolBackedExecutor implements ExecutorService {
    private final EnhancedThreadPoolExecutor sharedPool;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final List<FutureTask<?>> submittedTasks = Collections.synchronizedList(new LinkedList<>());
    private final Object taskCompletionLock = new Object();

    public SharedThreadPoolBackedExecutor(EnhancedThreadPoolExecutor sharedPool) {
        this.sharedPool = sharedPool;
    }

    @Override
    public void shutdown() {
        // we don't want to shut down the main sync thread pool
        shutdown.set(true);
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();

        // cancel all tasks so they are not executed from the shared task queue
        // if they are successfully cancelled, remove them from submittedTasks
        submittedTasks.removeIf(task -> task.cancel(true));
        List<Runnable> runningTasks = new ArrayList<>(submittedTasks);
        submittedTasks.clear();

        return runningTasks;
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get();
    }

    @Override
    public boolean isTerminated() {
        return shutdown.get() && submittedTasks.isEmpty();
    }

    @Override
    public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {
        long timeoutTime = System.currentTimeMillis() + timeUnit.toMillis(l);
        while (true) {
            long remainingTime = timeoutTime - System.currentTimeMillis();
            if (remainingTime <= 0) return false; // we timed out
            synchronized (taskCompletionLock) {
                if (shutdown.get() && submittedTasks.isEmpty()) return true;
                // there are still active tasks - wait until another task completes before checking again
                taskCompletionLock.wait(remainingTime); // don't wait longer than our original timeout
            }
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        return internalSubmit(new FutureTask<>(callable));
    }

    @Override
    public Future<?> submit(Runnable runnable) {
        return internalSubmit(new FutureTask<>(runnable, null));
    }

    @Override
    public <T> Future<T> submit(Runnable runnable, T t) {
        return internalSubmit(new FutureTask<>(runnable, t));
    }

    @Override
    public void execute(Runnable runnable) {
        internalSubmit(new FutureTask<>(runnable, null));
    }

    protected <T> Future<T> internalSubmit(FutureTask<T> futureTask) {
        if (shutdown.get()) throw new RejectedExecutionException("executor is shutdown");

        Future<T> future = sharedPool.blockingSubmit(() -> {
            try {
                futureTask.run();
                return futureTask.get();
            } finally {
                submittedTasks.remove(futureTask);
                synchronized (taskCompletionLock) {
                    taskCompletionLock.notify();
                }
            }
        });
        submittedTasks.add(futureTask);
        return future;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }
}

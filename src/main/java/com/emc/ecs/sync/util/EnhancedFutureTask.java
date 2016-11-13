package com.emc.ecs.sync.util;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * Allows access to the underlying Runnable or Callable, which FutureTask does not. This allows interrogation
 * of the tasks in the executor's queue
 */
public class EnhancedFutureTask<V> extends FutureTask<V> {
    private Callable<V> callable;
    private Runnable runnable;

    public EnhancedFutureTask(Callable<V> callable) {
        super(callable);
        this.callable = callable;
    }

    public EnhancedFutureTask(Runnable runnable, V result) {
        super(runnable, result);
        this.runnable = runnable;
    }

    public Callable<V> getCallable() {
        return callable;
    }

    public Runnable getRunnable() {
        return runnable;
    }
}

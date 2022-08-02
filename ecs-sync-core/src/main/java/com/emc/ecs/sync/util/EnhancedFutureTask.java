/*
 * Copyright (c) 2016-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

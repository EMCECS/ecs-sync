/*
 * Copyright (c) 2018 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package sync.ui.migration

abstract class AbstractMigrationTask<A extends AbstractMigration> implements Runnable {
    int taskWeight = 1
    int percentComplete
    TaskStatus taskStatus = TaskStatus.New
    String taskMessage
    long startTime
    long stopTime
    abstract name
    abstract description
    A migration

    long getDuration() {
        startTime ? (stopTime ?: System.currentTimeMillis()) - startTime : 0
    }

    abstract void execute()

    @Override
    final void run() {
        taskStatus = TaskStatus.Running
        startTime = System.currentTimeMillis()

        try {
            execute()
        } finally {
            stopTime = System.currentTimeMillis()
        }
    }
}

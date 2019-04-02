/*
 * Copyright 2013-2018 EMC Corporation. All Rights Reserved.
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

import groovy.util.logging.Slf4j
import sync.ui.storage.StorageEntry

@Slf4j
abstract class AbstractMigration<T extends AbstractMigrationConfig> implements Runnable {
    T migrationConfig

    State state = State.Initialized
    long startTime
    long resumeTime
    long stopTime
    long runTime

    abstract testConfig()

    abstract List<? extends AbstractMigrationTask> taskList

    def syncJobService
    def configService
    def historyService

    def percentComplete() {
        def totalWeightPercent = getTaskList().sum { it.taskWeight * 100 }
        def totalCompletionPercent = getTaskList().sum { it.taskWeight * it.percentComplete }
        (totalCompletionPercent * 100 / totalWeightPercent) as long
    }

    def isRunning() { state in [State.Running, State.Stopping] }

    def pause() {
        if (state == State.Initialized) throw new IllegalStateException("Cannot pause a migration job before it starts")
        if (state in [State.Stopping, State.Stopped]) throw new IllegalStateException("Cannot pause a job that has been stopped")
        if (state == State.Paused) return false
        state = State.Paused
        // accumulate runtime
        runTime = runTime + (System.currentTimeMillis() - resumeTime)
        true
    }

    def isPaused() { state == State.Paused }

    def resume() {
        if (state == State.Initialized) throw new IllegalStateException("Cannot pause a migration job before it starts")
        if (state in [State.Stopping, State.Stopped]) throw new IllegalStateException("Cannot pause a job that has been stopped")
        if (state == State.Running) return false
        state = State.Running
        resumeTime = System.currentTimeMillis()
        true
    }

    def stop() {
        if (state == State.Initialized) throw new IllegalStateException("Cannot stop a migration job before it starts")
        if (state in [State.Stopping, State.Stopped]) return false
        state = State.Stopping
        true
    }

    def isStopping() { state == State.Stopping }

    def isStopped() { state == State.Stopped }

    def duration() {
        if (isPaused()) runTime
        else if (isStopped()) runTime + (stopTime - resumeTime)
        else runTime + (System.currentTimeMillis() - resumeTime)
    }

    @Override
    void run() {
        state = State.Running
        startTime = resumeTime = System.currentTimeMillis()
        try {
            getTaskList().each {
                if (state == State.Running) it.run()
                if (it.taskStatus == TaskStatus.Error) {
                    log.warn("Task ${it.name} failed: ${it.taskMessage}... pausing to allow manual intervention")
                    pause()
                }
                while (isPaused()) {
                    sleep(2000)
                }
            }
        } catch (e) {
            log.error("Unexpected migration error", e)
        } finally {
            state = State.Stopped
            stopTime = System.currentTimeMillis()
        }
    }

    @Lazy
    volatile sourceStorage = new StorageEntry([configService: configService, xmlKey: migrationConfig.sourceXmlKey]).storage
    @Lazy
    volatile targetStorage = new StorageEntry([configService: configService, xmlKey: migrationConfig.targetXmlKey]).storage

    enum State {
        Initialized, Running, Paused, Stopping, Stopped
    }
}

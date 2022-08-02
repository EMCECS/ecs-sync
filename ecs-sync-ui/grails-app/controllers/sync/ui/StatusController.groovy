/*
 * Copyright (c) 2015-2019 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package sync.ui

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.rest.*
import org.slf4j.LoggerFactory

class StatusController implements ConfigAccessor {
    def rest
    def jobServer

    def index() {
        def config = configService.readConfig()
        JobList jobs = rest.get("${jobServer}/job") { accept(JobList.class) }.body as JobList
        def progressPercents = [:], msRemainings = [:]
        jobs.jobs.each {
            def jobId = it.jobId
            if (it.status == JobControlStatus.Complete) {
                progressPercents[jobId] = 100.toDouble()
                msRemainings[jobId] = 0L
            } else {
                def progress = SyncUtil.calculateProgress(it.progress)
                progressPercents[jobId] = progress * 100
                if (progressPercents[jobId] > 0)
                    msRemainings[jobId] = (it.progress.runtimeMs / progress - it.progress.runtimeMs).toLong()
            }
        }
        [
                lastJob         : SyncHistoryEntry.list(configService)[0],
                jobs            : jobs,
                progressPercents: progressPercents,
                msRemainings    : msRemainings,
                hostStats       : rest.get("${jobServer}/host") { accept(HostInfo.class) }.body as HostInfo,
                config          : config
        ]
    }

    def show() {
        def jobId = params.id ?: 1
        def response = rest.get("${jobServer}/job/${jobId}") { accept(SyncConfig.class) }

        if (response.status > 299) render status: response.status
        else {
            def config = configService.readConfig(false)
            SyncConfig sync = response.body as SyncConfig
            JobControl control = jobId ? rest.get("${jobServer}/job/${jobId}/control") { accept(JobControl.class) }.body as JobControl : null
            SyncProgress progress = jobId ? rest.get("${jobServer}/job/${jobId}/progress") { accept(SyncProgress.class) }.body as SyncProgress : null
            def progressPercent = null
            def msRemaining = null
            if (progress && progress.totalObjectsExpected) {
                if (!progress.runError && control?.status == JobControlStatus.Complete) {
                    progressPercent = 100.toDouble()
                    msRemaining = 0L
                } else {
                    progressPercent = SyncUtil.calculateProgress(progress) * 100
                    msRemaining = (progress.runtimeMs / (progressPercent / 100) - progress.runtimeMs).toLong()
                }
            }
            HostInfo hostStats = rest.get("${jobServer}/host") { accept(HostInfo.class) }.body as HostInfo
            double overallCpu = 0.0d
            if (progress && progress.cpuTimeMs && hostStats.hostCpuCount && progress.runtimeMs) {
                overallCpu = 100d * progress.cpuTimeMs / hostStats.hostCpuCount / progress.runtimeMs
            }
            [
                    jobId          : jobId,
                    sync           : sync,
                    progress       : progress,
                    control        : control,
                    progressPercent: progressPercent,
                    msRemaining    : msRemaining,
                    overallCpu     : overallCpu,
                    memorySize     : Runtime.getRuntime().totalMemory(),
                    hostStats      : hostStats,
                    config         : config
            ]
        }
    }

    def resume() {
        rest.post("${jobServer}/job/${params.jobId}/control") {
            contentType "application/xml"
            body new JobControl(status: JobControlStatus.Running)
        }

        redirect action: params.fromAction ?: 'index', params: [id: params.jobId]
    }

    def pause() {
        rest.post("${jobServer}/job/${params.jobId}/control") {
            contentType "application/xml"
            body new JobControl(status: JobControlStatus.Paused)
        }

        redirect action: params.fromAction ?: 'index', params: [id: params.jobId]
    }

    def stop() {
        rest.post("${jobServer}/job/${params.jobId}/control") {
            contentType "application/xml"
            body new JobControl(status: JobControlStatus.Stopped)
        }

        redirect action: params.fromAction ?: 'index', params: [id: params.jobId]
    }

    def setThreads() {
        rest.post("${jobServer}/job/${params.jobId}/control") {
            contentType "application/xml"
            body new JobControl(threadCount: params.threadCount as int)
        }

        redirect action: params.fromAction ?: 'index', params: [id: params.jobId]
    }

    def setLogLevel() {
        rest.post("${jobServer}/host/logging?level=${params.logLevel}")

        def root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)
        def changeUiLevel = [
                'silent' : { root.setLevel(Level.OFF) },
                'quiet'  : { root.setLevel(Level.WARN) },
                'verbose': { root.setLevel(Level.INFO) },
                'debug'  : { root.setLevel(Level.DEBUG) }
        ]
        if (changeUiLevel[params.logLevel]) changeUiLevel[params.logLevel]()

        redirect action: params.fromAction ?: 'index', params: [id: params.jobId]
    }
}

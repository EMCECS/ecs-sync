package sync.ui

import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.rest.*

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
                def progress = calculateProgress(it.progress)
                progressPercents[jobId] = progress * 100
                if (progressPercents[jobId] > 0)
                    msRemainings[jobId] = (it.progress.runtimeMs / progress - it.progress.runtimeMs).toLong()
            }
        }
        [
                lastArchive     : ArchiveEntry.list(configService)[0],
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
            def config = configService.readConfig()
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
                    progressPercent = calculateProgress(progress) * 100
                    msRemaining = (progress.runtimeMs / (progressPercent / 100) - progress.runtimeMs).toLong()
                }
            }
            HostInfo hostStats = rest.get("${jobServer}/host") { accept(HostInfo.class) }.body as HostInfo
            double overallCpu = 0.0d
            if (progress && progress.cpuTimeMs && progress.runtimeMs && hostStats.hostCpuCount) {
                overallCpu = progress.cpuTimeMs / progress.runtimeMs / hostStats.hostCpuCount * 100
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

        redirect action: params.fromAction ?: 'index', params: [id: params.jobId]
    }

    private static double calculateProgress(progress) {
        // when byte *and* object estimates are available, progress is based on a weighted average of the two
        // percentages with the lesser value counted twice i.e.:
        // ( 2 * min(bytePercent, objectPercent) + max(bytePercent, objectPercent) ) / 3
        double byteRatio = 0, objectRatio = 0, completionRatio = 0
        long totalBytes = progress.totalBytesExpected.toLong() - progress.bytesSkipped.toLong()
        long totalObjects = progress.totalObjectsExpected.toLong() - progress.objectsSkipped.toLong()
        if (progress != null && progress.runtimeMs.toLong() > 0) {
            if (totalBytes > 0) {
                byteRatio = (double) progress.bytesComplete.toLong() / totalBytes
                completionRatio = byteRatio
            }
            if (totalObjects > 0) {
                objectRatio = (double) progress.objectsComplete.toLong() / totalObjects
                completionRatio = objectRatio
            }
            if (byteRatio > 0 && objectRatio > 0)
                completionRatio = (2 * Math.min(byteRatio, objectRatio) + Math.max(byteRatio, objectRatio)) / 3
        }
        return completionRatio
    }
}

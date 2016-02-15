package sync.ui

class StatusController {
    def rest
    def jobServer
    def ecsService

    def index() {
        def config = ecsService.readUiConfig()
        def jobs = rest.get("${jobServer}/job").xml
        def progressPercents = [:], msRemainings = [:]
        jobs.Job.each {
            def jobId = it.JobId.toLong()
            if (it.Status.text() == 'Complete') {
                progressPercents[jobId] = 100.toDouble()
                msRemainings[jobId] = 0L
            } else {
                progressPercents[jobId] = calculateProgress(it.Progress) * 100
                if (progressPercents[jobId] > 0)
                    msRemainings[jobId] = (it.Progress.RuntimeMs.toDouble() / (progressPercents[jobId] / 100) - it.Progress.RuntimeMs.toDouble()).toLong()
            }
        }
        [
                lastResult      : ResultEntry.list(ecsService).first(),
                jobs            : jobs,
                progressPercents: progressPercents,
                msRemainings    : msRemainings,
                hostStats       : rest.get("${jobServer}/host").xml,
                config          : config
        ]
    }

    def show() {
        def jobId = params.id ?: 1
        def response = rest.get("${jobServer}/job/${jobId}")

        if (response.status > 299) render status: response.status
        else {
            def config = ecsService.readUiConfig()
            def sync = response.xml
            def control = jobId ? rest.get("${jobServer}/job/${jobId}/control").xml : null
            def progress = jobId ? rest.get("${jobServer}/job/${jobId}/progress").xml : null
            def progressPercent = null
            def msRemaining = null
            if (progress && progress.TotalObjectsExpected.toLong()) {
                if (progress.ObjectsComplete.toLong() && control?.Status?.text() == 'Complete') {
                    progressPercent = 100.toDouble()
                    msRemaining = 0L
                } else {
                    progressPercent = calculateProgress(progress) * 100
                    msRemaining = (progress.RuntimeMs.toDouble() / (progressPercent / 100) - progress.RuntimeMs.toDouble()).toLong()
                }
            }
            def hostStats = rest.get("${jobServer}/host").xml
            def overallCpu = 0.0d
            if (progress && progress.CpuTimeMs.text() && progress.RuntimeMs.toLong() && hostStats.HostCpuCount.toLong()) {
                overallCpu = progress.CpuTimeMs.toDouble() / progress.RuntimeMs.toDouble() / hostStats.HostCpuCount.toDouble() * 100
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
            xml {
                JobControl { Status('Running') }
            }
        }

        redirect action: params.fromAction ?: 'index'
    }

    def pause() {
        rest.post("${jobServer}/job/${params.jobId}/control") {
            xml {
                JobControl { Status('Paused') }
            }
        }

        redirect action: params.fromAction ?: 'index'
    }

    def stop() {
        rest.post("${jobServer}/job/${params.jobId}/control") {
            xml {
                JobControl { Status('Stopped') }
            }
        }

        redirect action: params.fromAction ?: 'index'
    }

    def setThreads() {
        rest.post("${jobServer}/job/${params.jobId}/control") {
            xml {
                JobControl {
                    SyncThreadCount(params.threadCount)
                    QueryThreadCount(params.threadCount)
                }
            }
        }

        redirect action: 'index'
    }

    private static double calculateProgress(progress) {
        // when byte *and* object estimates are available, progress is based on a weighted average of the two
        // percentages with the lesser value counted twice i.e.:
        // ( 2 * min(bytePercent, objectPercent) + max(bytePercent, objectPercent) ) / 3
        double byteRatio = 0, objectRatio = 0, completionRatio = 0;
        if (progress != null && progress.RuntimeMs.toLong() > 0) {
            if (progress.TotalBytesExpected.toLong() > 0) {
                byteRatio = (double) progress.BytesComplete.toLong() / progress.TotalBytesExpected.toLong();
                completionRatio = byteRatio;
            }
            if (progress.TotalObjectsExpected.toLong() > 0) {
                objectRatio = (double) progress.ObjectsComplete.toLong() / progress.TotalObjectsExpected.toLong();
                completionRatio = objectRatio;
            }
            if (byteRatio > 0 && objectRatio > 0)
                completionRatio = (2 * Math.min(byteRatio, objectRatio) + Math.max(byteRatio, objectRatio)) / 3;
        }
        return completionRatio;
    }
}

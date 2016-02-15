package sync.ui

import com.emc.ecs.sync.rest.JobControl
import com.emc.ecs.sync.rest.JobControlStatus
import com.emc.ecs.sync.rest.JobList

class SyncCleanupJob implements Mailer {
    def rest
    def jobServer
    def ecsService
    def reportService

    def execute() {
        JobList jobList = rest.get("${jobServer}/job") { accept(JobList.class) }.body
        def uiConfig = ecsService.readUiConfig()

        jobList.jobs.each {
            JobControl jobControl = rest.get("${jobServer}/job/${it.jobId}/control") { accept(JobControl.class) }.body
            if (jobControl.status in [JobControlStatus.Complete, JobControlStatus.Stopped]) {
                def syncEntry = reportService.generateReport(it.jobId)
                rest.delete("${jobServer}/job/${it.jobId}")

                if (syncEntry.syncResult.config.name) { // this sync was scheduled
                    def scheduleEntry = new ScheduleEntry([ecsService: ecsService, name: syncEntry.syncResult.config.name])
                    if (syncEntry.syncResult.progress.runError || syncEntry.syncResult.progress.objectsFailed) {
                        if (scheduleEntry.scheduledSync.alerts.onError)
                            simpleMail(uiConfig.alertEmail,
                                    "ECS Sync - '${scheduleEntry.name}' finished with errors",
                                    "Scheduled sync '${scheduleEntry.name}' completed, but with errors.\n" +
                                            "general error: ${syncEntry.syncResult.progress.runError}\n" +
                                            "total transfer errors: ${syncEntry.syncResult.progress.objectsFailed}")
                    } else {
                        if (scheduleEntry.scheduledSync.alerts.onComplete)
                            simpleMail(uiConfig.alertEmail,
                                    "ECS Sync - '${scheduleEntry.name}' completed successfully",
                                    "Scheduled sync '${scheduleEntry.name}' successfully completed on " +
                                            "${new Date(syncEntry.syncResult.progress.syncStopTime).format("yyyy-MM-dd 'at' HH:mm:ss z")}")
                    }
                }
            }
        }
    }
}

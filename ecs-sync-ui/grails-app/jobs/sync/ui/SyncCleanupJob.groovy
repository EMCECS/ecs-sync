package sync.ui

import com.emc.ecs.sync.rest.JobControl
import com.emc.ecs.sync.rest.JobControlStatus
import com.emc.ecs.sync.rest.JobList

class SyncCleanupJob implements Mailer, ConfigAccessor {
    def rest
    def jobServer
    def historyService

    def execute() {
        JobList jobList = rest.get("${jobServer}/job") { accept(JobList.class) }.body
        def uiConfig = configService.readConfig()

        jobList.jobs.each {
            JobControl jobControl = rest.get("${jobServer}/job/${it.jobId}/control") { accept(JobControl.class) }.body
            if (jobControl.status in [JobControlStatus.Complete, JobControlStatus.Stopped]) {
                def historyEntry = historyService.archiveJob(it.jobId)
                if (SyncUtil.generatedTable(historyEntry.syncResult.config)) rest.delete("${jobServer}/job/${it.jobId}")
                else rest.delete("${jobServer}/job/${it.jobId}?keepDatabase=true")

                if (historyEntry.syncResult.config.properties.scheduleName) { // this sync was scheduled
                    def scheduleEntry = new ScheduleEntry([configService: configService, name: historyEntry.syncResult.config.properties.scheduleName])
                    if (historyEntry.syncResult.progress.runError || historyEntry.syncResult.progress.objectsFailed) {
                        if (scheduleEntry.scheduledSync.alerts.onError)
                            simpleMail(uiConfig.alertEmail,
                                    "ECS Sync - '${scheduleEntry.name}' finished with errors",
                                    "Scheduled sync '${scheduleEntry.name}' completed, but with errors.\n" +
                                            "general error: ${historyEntry.syncResult.progress.runError}\n" +
                                            "total transfer errors: ${historyEntry.syncResult.progress.objectsFailed}")
                    } else {
                        if (scheduleEntry.scheduledSync.alerts.onComplete)
                            simpleMail(uiConfig.alertEmail,
                                    "ECS Sync - '${scheduleEntry.name}' completed successfully",
                                    "Scheduled sync '${scheduleEntry.name}' successfully completed on " +
                                            "${new Date(historyEntry.syncResult.progress.syncStopTime).format("yyyy-MM-dd 'at' HH:mm:ss z")}")
                    }
                }
            }
        }
    }
}

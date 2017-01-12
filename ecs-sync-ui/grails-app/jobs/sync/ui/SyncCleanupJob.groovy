package sync.ui

import com.emc.ecs.sync.rest.JobControl
import com.emc.ecs.sync.rest.JobControlStatus
import com.emc.ecs.sync.rest.JobList

class SyncCleanupJob implements Mailer, ConfigAccessor {
    def rest
    def jobServer
    def archiveService

    def execute() {
        JobList jobList = rest.get("${jobServer}/job") { accept(JobList.class) }.body
        def uiConfig = configService.readConfig()

        jobList.jobs.each {
            JobControl jobControl = rest.get("${jobServer}/job/${it.jobId}/control") { accept(JobControl.class) }.body
            if (jobControl.status in [JobControlStatus.Complete, JobControlStatus.Stopped]) {
                def archiveEntry = archiveService.archiveJob(it.jobId)
                if (SyncUtil.generatedTable(archiveEntry.syncResult.config)) rest.delete("${jobServer}/job/${it.jobId}")
                else rest.delete("${jobServer}/job/${it.jobId}?keepDatabase=true")

                if (archiveEntry.syncResult.config.properties.scheduleName) { // this sync was scheduled
                    def scheduleEntry = new ScheduleEntry([configService: configService, name: archiveEntry.syncResult.config.properties.scheduleName])
                    if (archiveEntry.syncResult.progress.runError || archiveEntry.syncResult.progress.objectsFailed) {
                        if (scheduleEntry.scheduledSync.alerts.onError)
                            simpleMail(uiConfig.alertEmail,
                                    "ECS Sync - '${scheduleEntry.name}' finished with errors",
                                    "Scheduled sync '${scheduleEntry.name}' completed, but with errors.\n" +
                                            "general error: ${archiveEntry.syncResult.progress.runError}\n" +
                                            "total transfer errors: ${archiveEntry.syncResult.progress.objectsFailed}")
                    } else {
                        if (scheduleEntry.scheduledSync.alerts.onComplete)
                            simpleMail(uiConfig.alertEmail,
                                    "ECS Sync - '${scheduleEntry.name}' completed successfully",
                                    "Scheduled sync '${scheduleEntry.name}' successfully completed on " +
                                            "${new Date(archiveEntry.syncResult.progress.syncStopTime).format("yyyy-MM-dd 'at' HH:mm:ss z")}")
                    }
                }
            }
        }
    }
}

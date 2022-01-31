/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package sync.ui

import com.emc.ecs.sync.rest.JobControl
import com.emc.ecs.sync.rest.JobList

class SyncCleanupJob implements Mailer, ConfigAccessor {
    def sessionRequired = false

    def rest
    def jobServer
    def historyService

    def execute() {
        JobList jobList = rest.get("${jobServer}/job") { accept(JobList.class) }.body
        def uiConfig = configService.readConfig()

        jobList.jobs.each {
            JobControl jobControl = rest.get("${jobServer}/job/${it.jobId}/control") { accept(JobControl.class) }.body
            if (jobControl.status?.finalState) {
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

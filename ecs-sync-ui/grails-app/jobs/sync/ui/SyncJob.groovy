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

import grails.core.GrailsApplication

class SyncJob implements Mailer, ConfigAccessor {
    def syncJobService
    GrailsApplication grailsApplication

    def execute(context) {
        def name = context.mergedJobDataMap.name

        ScheduleEntry scheduleEntry = new ScheduleEntry([configService: configService, name: name])

        SyncUtil.configureDatabase(scheduleEntry.scheduledSync.config, grailsApplication)

        def result = syncJobService.submitJob(scheduleEntry.scheduledSync.config)

        def uiConfig = configService.readConfig()
        if (result.success) {
            if (scheduleEntry.scheduledSync.alerts.onStart)
                simpleMail(uiConfig.alertEmail,
                        "ECS Sync - '${name}' started successfully",
                        "Scheduled sync '${name}' was successfully submitted to ECS Sync on ${new Date().format("yyyy-MM-dd 'at' HH:mm:ss z")}")
        } else {
            if (scheduleEntry.scheduledSync.alerts.onError)
                simpleMail(uiConfig.alertEmail,
                        "ECS Sync - '${name}' failed to start",
                        "sync service rejected the job: ${result.statusCode} - ${result.statusText} - ${result.message}")
        }
    }
}

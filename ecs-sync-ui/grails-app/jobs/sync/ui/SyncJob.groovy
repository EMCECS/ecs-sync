/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import grails.core.GrailsApplication
import groovy.util.logging.Slf4j

@Slf4j
class SyncJob implements Mailer, ConfigAccessor {
    def sessionRequired = false

    def syncJobService
    GrailsApplication grailsApplication

    def executeShell(cmd) {
        def out = new StringBuilder(), err = new StringBuilder()
        def proc = cmd.execute()
        proc.consumeProcessOutput(out, err)
        proc.waitFor()
        def exitValue = proc.exitValue()
        if(exitValue != 0) {
            log.error("Command ${cmd} exited with value ${exitValue} \nstderr: ${err} \nstdout: ${out}")
            throw new RuntimeException("${cmd} failed with exit value ${exitValue}, Error: ${err}")
        }
        return out
    }

    def execute(context) {
        def name = context.mergedJobDataMap.name
        def uiConfig = configService.readConfig()

        ScheduleEntry scheduleEntry = new ScheduleEntry([configService: configService, name: name])

        if (scheduleEntry.scheduledSync.preCheckScript) {
            try {
                executeShell(scheduleEntry.scheduledSync.preCheckScript)
            }catch (Exception e) {
                log.error("Scheduled Sync Job ${name} wasn't started because precheck script failed: ${e}")
                if (scheduleEntry.scheduledSync.alerts.onError)
                    simpleMail(uiConfig.alertEmail,
                            "ECS Sync - '${name}' failed to start",
                            "Precheck Script failed : ${e}")
                return
            }
        }

        SyncUtil.configureDatabase(scheduleEntry.scheduledSync.config, grailsApplication)

        def result = syncJobService.submitJob(scheduleEntry.scheduledSync.config)

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

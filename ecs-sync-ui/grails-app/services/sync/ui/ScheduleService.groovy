/*
 * Copyright (c) 2016-2017 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import org.quartz.CronScheduleBuilder
import org.quartz.JobDataMap
import org.quartz.TriggerBuilder
import org.quartz.impl.matchers.GroupMatcher

class ScheduleService implements ConfigAccessor {
    static syncGroup = 'sync-jobs'
    static cleanupGroup = 'cleanup-jobs'
    static cleanupJobName = 'sync-cleanup'

    def quartzScheduler

    def scheduleAllJobs() {
        // first, unschedule jobs
        SyncCleanupJob.unschedule(cleanupJobName, cleanupGroup)
        quartzScheduler.getTriggerKeys(GroupMatcher.triggerGroupEquals(syncGroup)).each {
            quartzScheduler.unscheduleJob(it)
        }

        def uiConfig = configService.readConfig()

        if (uiConfig.autoArchive) {
            SyncCleanupJob.schedule(TriggerBuilder.newTrigger().withIdentity(cleanupJobName, cleanupGroup)
                    .withSchedule(CronScheduleBuilder.cronSchedule('0/5 * * ? * *')).build())
        }

        ScheduleEntry.list(configService).each { entry ->
            def hour = entry.scheduledSync.startHour, minute = entry.scheduledSync.startMinute
            def days = entry.scheduledSync.daysOfWeek.collect { it.ordinal() + 1 }.join(',')
            SyncJob.schedule(TriggerBuilder.newTrigger().withIdentity(entry.name, syncGroup)
                    .withSchedule(CronScheduleBuilder.cronSchedule("0 ${minute} ${hour} ? * ${days}"))
                    .usingJobData(new JobDataMap([name: entry.name])).build())
        }
    }
}

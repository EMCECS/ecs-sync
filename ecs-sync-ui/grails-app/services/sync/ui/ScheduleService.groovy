package sync.ui

import org.quartz.CronScheduleBuilder
import org.quartz.JobDataMap
import org.quartz.TriggerBuilder
import org.quartz.impl.matchers.GroupMatcher

class ScheduleService {
    static syncGroup = 'sync-jobs'
    static cleanupGroup = 'cleanup-jobs'
    static cleanupJobName = 'sync-cleanup'

    def ecsService
    def quartzScheduler

    def scheduleAllJobs() {
        // first, unschedule jobs
        SyncCleanupJob.unschedule(cleanupJobName, cleanupGroup)
        quartzScheduler.getTriggerKeys(GroupMatcher.triggerGroupEquals(syncGroup)).each {
            quartzScheduler.unscheduleJob(it)
        }

        def uiConfig = ecsService.readUiConfig()

        if (uiConfig.autoArchive) {
            SyncCleanupJob.schedule(TriggerBuilder.newTrigger().withIdentity(cleanupJobName, cleanupGroup)
                    .withSchedule(CronScheduleBuilder.cronSchedule('0/5 * * ? * *')).build())
        }

        ScheduleEntry.list(ecsService).each { entry ->
            def hour = entry.scheduledSync.startHour, minute = entry.scheduledSync.startMinute
            def days = entry.scheduledSync.daysOfWeek.collect { it.ordinal() + 1 }.join(',')
            SyncJob.schedule(TriggerBuilder.newTrigger().withIdentity(entry.name, syncGroup)
                    .withSchedule(CronScheduleBuilder.cronSchedule("0 ${minute} ${hour} ? * ${days}"))
                    .usingJobData(new JobDataMap([name: entry.name])).build())
        }
    }
}

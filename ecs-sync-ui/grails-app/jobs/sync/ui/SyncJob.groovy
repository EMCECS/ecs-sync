package sync.ui

import grails.core.GrailsApplication

class SyncJob implements Mailer {
    def rest
    def jobServer
    def configService
    GrailsApplication grailsApplication

    def execute(context) {
        def name = context.mergedJobDataMap.name

        ScheduleEntry scheduleEntry = new ScheduleEntry([configService: configService, name: name])

        SyncUtil.configureDatabase(scheduleEntry.scheduledSync.config, grailsApplication)

        def response = rest.put("${jobServer}/job") {
            contentType 'application/xml'
            body scheduleEntry.scheduledSync.config
        }

        def uiConfig = configService.readConfig()
        if (response.statusCode.is2xxSuccessful()) {
            if (scheduleEntry.scheduledSync.alerts.onStart)
                simpleMail(uiConfig.alertEmail,
                        "ECS Sync - '${name}' started successfully",
                        "Scheduled sync '${name}' was successfully submitted to ECS Sync on ${new Date().format("yyyy-MM-dd 'at' HH:mm:ss z")}")
        } else {
            if (scheduleEntry.scheduledSync.alerts.onError)
                simpleMail(uiConfig.alertEmail,
                        "ECS Sync - '${name}' failed to start",
                        "sync service rejected the job: ${response.status} - ${response.statusCode.reasonPhrase} - ${response.text}")
        }
    }
}

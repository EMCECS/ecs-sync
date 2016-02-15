package sync.ui

class SyncJob implements Mailer {
    def rest
    def jobServer
    def ecsService

    def execute(context) {
        def name = context.mergedJobDataMap.name

        ScheduleEntry scheduleEntry = new ScheduleEntry([ecsService: ecsService, name: name])

        def response = rest.put("${jobServer}/job") {
            contentType 'application/xml'
            body scheduleEntry.scheduledSync.config
        };

        def uiConfig = ecsService.readUiConfig()
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

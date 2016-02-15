package sync.ui

import com.emc.ecs.sync.rest.SyncConfig

class SyncController {
    static allowedMethods = [start: "POST"]

    def rest
    def jobServer
    def ecsService
    def reportService

    def create() {
        [
                uiConfig  : ecsService.readUiConfig(),
                syncConfig: new SyncConfig()
        ]
    }

    def start() {
        SyncConfig syncConfig = new SyncConfig()
        bindData(syncConfig, params.syncConfig)
        SyncUtil.correctBindingResult(syncConfig)

        def dbName = "sync_${new Date().format(ResultEntry.idFormat)}"
        try {
            SyncUtil.configureDatabase(syncConfig, params['syncConfig.dbType'], dbName, grailsApplication)
        } catch (Exception e) {
            flash.error = g.message(code: e.getMessage())
            render view: 'create', model: [
                    uiConfig  : ecsService.readUiConfig(),
                    syncConfig: syncConfig
            ]
            return
        }

        def response = rest.put("${jobServer}/job") {
            contentType 'application/xml'
            body syncConfig
        };
        if (!response.statusCode.is2xxSuccessful())
            throw new RuntimeException("${response.status}: ${response.statusCode.reasonPhrase}: ${response.text}")

        redirect controller: 'status', action: 'index'
    }

    def restart() {
        if (params.jobId) {
            def syncConfig = rest.get("${jobServer}/job/${params.jobId}").text

            reportService.generateReport(params.jobId)

            rest.delete("${jobServer}/job/${params.jobId}")

            rest.put("${jobServer}/job") {
                contentType 'application/xml'
                body syncConfig
            }
        }

        redirect controller: 'status', action: 'index'
    }

    def errors() {
        def filename = "sync-errors_${new Date().format(ResultEntry.idFormat)}.csv"
        response.setHeader('Content-Disposition', "attachment; filename=${filename}")
        def con = "${jobServer}/job/${params.jobId}/errors.csv".toURL().openConnection()
        response.contentType = con.getHeaderField('Content-Type')
        response.outputStream << con.inputStream
        response.outputStream.flush()
    }

    def syncReport() {
        if (params.jobId) {
            reportService.generateReport(params.jobId)

            rest.delete("${jobServer}/job/${params.jobId}")
        }

        redirect controller: 'status', action: 'index'
    }
}

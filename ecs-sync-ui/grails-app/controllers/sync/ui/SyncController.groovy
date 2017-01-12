package sync.ui

import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.rest.SyncProgress

class SyncController implements ConfigAccessor {
    static allowedMethods = [start: "POST"]

    def rest
    def jobServer
    def archiveService

    def create() {
        [
                uiConfig  : configService.readConfig(),
                syncConfig: new SyncConfig()
        ]
    }

    def start() {
        SyncConfig syncConfig
        try {
            syncConfig = SyncUtil.bindSyncConfig(this, null, params.syncConfig)
            SyncUtil.configureDatabase(syncConfig, grailsApplication)
        } catch (Exception e) {
            flash.error = g.message(code: e.getMessage())
            SyncUtil.coerceFilterParams(params.syncConfig)
            render view: 'create', model: [
                    uiConfig  : configService.readConfig(),
                    syncConfig: params.syncConfig
            ]
            return
        }

        def response = rest.put("${jobServer}/job") {
            contentType 'application/xml'
            body syncConfig
        }
        if (!response.statusCode.is2xxSuccessful())
            throw new RuntimeException("${response.status}: ${response.statusCode.reasonPhrase}: ${response.text}")

        redirect controller: 'status', action: 'index'
    }

    def restart() {
        if (params.jobId) {
            def syncConfig = rest.get("${jobServer}/job/${params.jobId}").text

            archiveService.archiveJob(params.jobId)

            rest.delete("${jobServer}/job/${params.jobId}?keepDatabase=true")

            rest.put("${jobServer}/job") {
                contentType 'application/xml'
                body syncConfig
            }
        }

        redirect controller: 'status', action: 'index'
    }

    def copyArchived() {
        if (params.archiveId) {
            def archiveEntry = new ArchiveEntry(id: params.archiveId, configService: configService)

            SyncUtil.resetGeneratedTable(archiveEntry.syncResult.config)

            render view: 'create', model: [
                    uiConfig  : configService.readConfig(),
                    syncConfig: archiveEntry.syncResult.config
            ]
        }
    }

    def errors() {
        SyncProgress progress = rest.get("${jobServer}/job/${params.jobId}/progress") {
            accept(SyncProgress.class)
        }.body as SyncProgress
        def archiveEntry = new ArchiveEntry([configService: configService, jobId: params.jobId.toLong(), startTime: new Date(progress.syncStartTime)])
        def filename = "${archiveEntry.id}_errors.csv"
        response.setHeader('Content-Disposition', "attachment; filename=${filename}")
        def con = "${jobServer}/job/${params.jobId}/errors.csv".toURL().openConnection()
        response.contentType = con.getHeaderField('Content-Type')
        response.outputStream << con.inputStream
        response.outputStream.flush()
    }

    def allObjectReport() {
        SyncProgress progress = rest.get("${jobServer}/job/${params.jobId}/progress") {
            accept(SyncProgress.class)
        }.body as SyncProgress
        def archiveEntry = new ArchiveEntry([configService: configService, jobId: params.jobId.toLong(), startTime: new Date(progress.syncStartTime)])
        def filename = "${archiveEntry.id}_all_objects.csv"
        response.setHeader('Content-Disposition', "attachment; filename=${filename}")
        def con = "${jobServer}/job/${params.jobId}/all-objects-report.csv".toURL().openConnection()
        response.contentType = con.getHeaderField('Content-Type')
        response.outputStream << con.inputStream
        response.outputStream.flush()
    }

    def archive() {
        if (params.jobId) {
            archiveService.archiveJob(params.jobId)

            def syncConfig = rest.get("${jobServer}/job/${params.jobId}") {
                accept(SyncConfig.class)
            }.body as SyncConfig
            if (SyncUtil.generatedTable(syncConfig)) rest.delete("${jobServer}/job/${params.jobId}")
            else rest.delete("${jobServer}/job/${params.jobId}?keepDatabase=true")
        }

        redirect controller: 'status', action: 'index'
    }
}

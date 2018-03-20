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

import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.rest.SyncProgress

class SyncController implements ConfigAccessor {
    static allowedMethods = [start: "POST"]

    def rest
    def jobServer
    def historyService

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

            historyService.archiveJob(params.jobId)

            rest.delete("${jobServer}/job/${params.jobId}?keepDatabase=true")

            rest.put("${jobServer}/job") {
                contentType 'application/xml'
                body syncConfig
            }
        }

        redirect controller: 'status', action: 'index'
    }

    def copyArchived() {
        if (params.entryId) {
            def historyEntry = new HistoryEntry(id: params.entryId, configService: configService)

            SyncUtil.resetGeneratedTable(historyEntry.syncResult.config)

            render view: 'create', model: [
                    uiConfig  : configService.readConfig(),
                    syncConfig: historyEntry.syncResult.config
            ]
        }
    }

    def errors() {
        SyncProgress progress = rest.get("${jobServer}/job/${params.jobId}/progress") {
            accept(SyncProgress.class)
        }.body as SyncProgress
        def historyEntry = new HistoryEntry([configService: configService, jobId: params.jobId.toLong(), startTime: new Date(progress.syncStartTime)])
        def filename = "${historyEntry.id}_errors.csv"
        response.setHeader('Content-Disposition', "attachment; filename=${filename}")
        def con = "${jobServer}/job/${params.jobId}/errors.csv".toURL().openConnection()
        response.contentType = con.getHeaderField('Content-Type')
        response.outputStream << con.inputStream
        response.outputStream.flush()
    }

    def retries() {
        SyncProgress progress = rest.get("${jobServer}/job/${params.jobId}/progress") {
            accept(SyncProgress.class)
        }.body as SyncProgress
        def historyEntry = new HistoryEntry([configService: configService, jobId: params.jobId.toLong(), startTime: new Date(progress.syncStartTime)])
        def filename = "${historyEntry.id}_retries.csv"
        response.setHeader('Content-Disposition', "attachment; filename=${filename}")
        def con = "${jobServer}/job/${params.jobId}/retries.csv".toURL().openConnection()
        response.contentType = con.getHeaderField('Content-Type')
        response.outputStream << con.inputStream
        response.outputStream.flush()
    }

    def allObjectReport() {
        SyncProgress progress = rest.get("${jobServer}/job/${params.jobId}/progress") {
            accept(SyncProgress.class)
        }.body as SyncProgress
        def historyEntry = new HistoryEntry([configService: configService, jobId: params.jobId.toLong(), startTime: new Date(progress.syncStartTime)])
        def filename = "${historyEntry.id}_all_objects.csv"
        response.setHeader('Content-Disposition', "attachment; filename=${filename}")
        def con = "${jobServer}/job/${params.jobId}/all-objects-report.csv".toURL().openConnection()
        response.contentType = con.getHeaderField('Content-Type')
        response.outputStream << con.inputStream
        response.outputStream.flush()
    }

    def archive() {
        if (params.jobId) {
            historyService.archiveJob(params.jobId)

            def syncConfig = rest.get("${jobServer}/job/${params.jobId}") {
                accept(SyncConfig.class)
            }.body as SyncConfig
            if (SyncUtil.generatedTable(syncConfig)) rest.delete("${jobServer}/job/${params.jobId}")
            else rest.delete("${jobServer}/job/${params.jobId}?keepDatabase=true")
        }

        redirect controller: 'status', action: 'index'
    }
}

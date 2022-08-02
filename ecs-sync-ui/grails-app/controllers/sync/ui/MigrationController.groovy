/*
 * Copyright (c) 2018 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import grails.converters.JSON
import sync.ui.migration.MigrationEntry
import sync.ui.storage.StorageEntry

import static org.springframework.http.HttpStatus.CREATED

class MigrationController implements ConfigAccessor {
    def migrationService
    def historyService

    def index() {
        def entries = MigrationEntry.list(configService)
        [
                migrationEntries: entries,
                activeMigrations: migrationService.listActive()
        ]
    }

    def show() {
        def entry = new MigrationEntry([configService: configService, xmlKey: params.migrationKey])
        [
                migrationEntry: entry,
                activeMigration: migrationService.lookup(entry.guid, false)
        ]
    }

    def create() {
        def entry = new MigrationEntry([configService: configService, type: params.type])
        [
                migrationEntry: entry,
                config: configService.readConfig(),
                sourceEntries: StorageEntry.list(configService).findAll({ it.type == entry.migrationConfig.sourceStorageType }),
                targetEntries: StorageEntry.list(configService).findAll({ it.type == entry.migrationConfig.targetStorageType })
        ]
    }

    def test(MigrationEntry migrationEntry) {
        render migrationService.testConfig(migrationEntry.migrationConfig) as JSON
    }

    def start(MigrationEntry migrationEntry) {
        if (migrationEntry.hasErrors()) {
            render view: 'create', model: [
                    migrationEntry: migrationEntry,
                    config: configService.readConfig(),
                    sourceEntries: StorageEntry.list(configService).findAll({ it.type == migrationEntry.migrationConfig.sourceStorageType }),
                    targetEntries: StorageEntry.list(configService).findAll({ it.type == migrationEntry.migrationConfig.targetStorageType })
            ]
            return
        }

        migrationEntry.configService = configService // TODO: this is probably bad form

        migrationEntry.write()

        migrationService.start(migrationEntry.migrationConfig)

        request.withFormat {
            form multipartForm {
                flash.message = 'migration started'
                redirect action: 'index', method: 'GET'
            }
            '*' { respond migrationEntry, [status: CREATED] }
        }
    }

    def restart() {
        migrationService.start(new MigrationEntry([xmlKey: params.migrationKey, configService: configService]).migrationConfig)
        redirect action: params.fromAction ?: 'index', params: [migrationKey: params.migrationKey]
    }

    def pause() {
        migrationService.pause(new MigrationEntry([xmlKey: params.migrationKey]).guid)
        redirect action: params.fromAction ?: 'index', params: [migrationKey: params.migrationKey]
    }

    def resume() {
        migrationService.resume(new MigrationEntry([xmlKey: params.migrationKey]).guid)
        redirect action: params.fromAction ?: 'index', params: [migrationKey: params.migrationKey]
    }

    def stop() {
        migrationService.stop(new MigrationEntry([xmlKey: params.migrationKey]).guid)
        redirect action: params.fromAction ?: 'index', params: [migrationKey: params.migrationKey]
    }

    def archive() {
        def migrationEntry = new MigrationEntry([xmlKey: params.migrationKey])
        historyService.archiveMigration(migrationEntry.guid)
        migrationService.delete(migrationEntry.guid)
        deleteEntry(params.migrationKey)
        redirect action: 'index'
    }

    private deleteEntry(xmlKey) {
        new MigrationEntry([xmlKey: xmlKey]).allKeys.each { configService.deleteConfigObject(it) }
    }
}

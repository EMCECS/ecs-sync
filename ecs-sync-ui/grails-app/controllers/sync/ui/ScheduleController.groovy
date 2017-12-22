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

import static org.springframework.http.HttpStatus.CREATED
import static org.springframework.http.HttpStatus.OK

class ScheduleController implements ConfigAccessor {
    static allowedMethods = [save: 'POST', update: 'PUT']

    def scheduleService

    def index() {
        [scheduleEntries: ScheduleEntry.list(configService)]
    }

    def create() {
        def scheduleEntry = new ScheduleEntry([configService: configService, scheduledSync: new ScheduledSync()])
        [
                uiConfig     : configService.readConfig(),
                scheduleEntry: scheduleEntry
        ]
    }

    def save(ScheduleEntry scheduleEntry) {
        try {
            scheduleEntry.scheduledSync.config = SyncUtil.bindSyncConfig(this, null, params.scheduleEntry.scheduledSync.config)
        } catch (Exception e) {
            scheduleEntry.errors.reject(null, e.getMessage())
            scheduleEntry.scheduledSync.config = params.scheduleEntry.scheduledSync?.config
            SyncUtil.coerceFilterParams(params.scheduleEntry.scheduledSync?.config)
        }

        if (scheduleEntry.exists()) scheduleEntry.errors.rejectValue('name', 'exists')

        if (scheduleEntry.hasErrors()) {
            render view: 'create', model: [
                    uiConfig     : configService.readConfig(),
                    scheduleEntry: scheduleEntry
            ]
            return
        }

        scheduleEntry.configService = configService // TODO: this is probably bad form
        // name our scheduled sync jobs
        scheduleEntry.scheduledSync.config.properties.scheduleName = scheduleEntry.name

        scheduleEntry.write()

        scheduleService.scheduleAllJobs()

        request.withFormat {
            form multipartForm {
                flash.message = 'sync scheduled'
                redirect action: 'index', method: 'GET'
            }
            '*' { respond scheduleEntry, [status: CREATED] }
        }
    }

    def edit(String name) {
        if (name == null) redirect action: 'create'
        def scheduleEntry = new ScheduleEntry([configService: configService, name: name])
        [
                uiConfig     : configService.readConfig(),
                scheduleEntry: scheduleEntry
        ]
    }

    def update(ScheduleEntry scheduleEntry) {
        try {
            scheduleEntry.scheduledSync.config = SyncUtil.bindSyncConfig(this, null, params.scheduleEntry.scheduledSync.config)
        } catch (Exception e) {
            scheduleEntry.errors.reject(null, e.getMessage())
            scheduleEntry.scheduledSync.config = params.scheduleEntry.scheduledSync?.config
            SyncUtil.coerceFilterParams(params.scheduleEntry.scheduledSync?.config)
        }

        def oldName = params.scheduleEntry.old_name
        if (oldName != scheduleEntry.name && scheduleEntry.exists()) scheduleEntry.errors.rejectValue('name', 'exists', [scheduleEntry.name] as Object[], null)

        if (scheduleEntry.hasErrors()) {
            render view: 'edit', model: [
                    uiConfig     : configService.readConfig(),
                    scheduleEntry: scheduleEntry
            ]
            return
        }

        scheduleEntry.configService = configService // TODO: this is probably bad form
        // name our scheduled sync jobs
        scheduleEntry.scheduledSync.config.properties.scheduleName = scheduleEntry.name

        scheduleEntry.write()

        if (oldName != scheduleEntry.name) deleteSchedule(oldName)

        scheduleService.scheduleAllJobs()

        request.withFormat {
            form multipartForm {
                flash.message = 'schedule updated'
                redirect action: 'index', method: 'GET'
            }
            '*' { respond scheduleEntry, [status: OK] }
        }
    }

    def delete(String name) {
        deleteSchedule(name)

        scheduleService.scheduleAllJobs() // TODO: make this service more granular

        flash.message = 'schedule deleted'
        redirect action: 'index'
    }

    private deleteSchedule(String name) {
        new ScheduleEntry([name: name]).allKeys.each { configService.deleteConfigObject(it) }
    }
}

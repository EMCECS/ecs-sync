package sync.ui

import static org.springframework.http.HttpStatus.CREATED
import static org.springframework.http.HttpStatus.OK

class ScheduleController {
    static allowedMethods = [save: 'POST', update: 'PUT']

    def ecsService
    def scheduleService

    def index() {
        [scheduleEntries: ScheduleEntry.list(ecsService)]
    }

    def create() {
        def scheduleEntry = new ScheduleEntry([ecsService: ecsService, scheduledSync: new ScheduledSync()])
        [
                uiConfig     : ecsService.readUiConfig(),
                scheduleEntry: scheduleEntry
        ]
    }

    def save(ScheduleEntry scheduleEntry) {
        postBinding(params, scheduleEntry)

        if (scheduleEntry.exists()) scheduleEntry.errors.rejectValue('name', 'exists')

        if (scheduleEntry.hasErrors()) {
            render view: 'create', model: [
                    uiConfig     : ecsService.readUiConfig(),
                    scheduleEntry: scheduleEntry
            ]
            return
        }

        // name our scheduled sync jobs
        scheduleEntry.scheduledSync.config.name = scheduleEntry.name

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
        def scheduleEntry = new ScheduleEntry([ecsService: ecsService, name: name])
        [
                uiConfig     : ecsService.readUiConfig(),
                scheduleEntry: scheduleEntry
        ]
    }

    def update(ScheduleEntry scheduleEntry) {
        postBinding(params, scheduleEntry)

        def oldName = params.scheduleEntry.old_name
        if (oldName != scheduleEntry.name && scheduleEntry.exists()) scheduleEntry.errors.rejectValue('name', 'exists')

        if (scheduleEntry.hasErrors()) {
            render view: 'edit', model: [
                    uiConfig     : ecsService.readUiConfig(),
                    scheduleEntry: scheduleEntry
            ]
            return
        }

        // name our scheduled sync jobs
        scheduleEntry.scheduledSync.config.name = scheduleEntry.name

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

    private postBinding(params, ScheduleEntry scheduleEntry) {
        SyncUtil.correctBindingResult(scheduleEntry?.scheduledSync?.config)

        try {
            SyncUtil.configureDatabase(scheduleEntry.scheduledSync.config, params['scheduleEntry.scheduledSync.config.dbType'],
                    scheduleEntry.name, grailsApplication)
        } catch (Exception e) {
            scheduleEntry.errors.reject(e.getMessage())
        }

        scheduleEntry.ecsService = ecsService // TODO: this is probably bad form
    }

    private deleteSchedule(String name) {
        new ScheduleEntry([name: name]).allKeys.each { ecsService.deleteConfigObject(it) }
    }
}

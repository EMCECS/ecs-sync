package sync.ui

import sync.ui.storage.AtmosStorage
import sync.ui.storage.EcsStorage
import sync.ui.storage.StorageEntry

import static org.springframework.http.HttpStatus.CREATED
import static org.springframework.http.HttpStatus.OK

class StorageController implements ConfigAccessor {
    def atmosService
    def ecsService

    def index() {
        def entries = StorageEntry.list(configService)
        [storageEntries: entries]
    }

    def create() {
        def storageEntry = new StorageEntry([configService: configService, type: params.type])
        [
                storageEntry: storageEntry
        ]
    }

    def save(StorageEntry storageEntry) {
        if (storageEntry.exists()) storageEntry.errors.rejectValue('name', 'exists')

        if (storageEntry.hasErrors()) {
            render view: 'create', model: [
                    storageEntry: storageEntry
            ]
            return
        }

        storageEntry.configService = configService // TODO: this is probably bad form

        storageEntry.write()

        request.withFormat {
            form multipartForm {
                flash.message = 'storage cluster saved'
                redirect action: 'index', method: 'GET'
            }
            '*' { respond storageEntry, [status: CREATED] }
        }
    }

    def edit(String storageKey) {
        if (storageKey == null) redirect action: 'create'
        def storageEntry = new StorageEntry([configService: configService, xmlKey: storageKey])
        [
                storageEntry: storageEntry
        ]
    }

    def update(StorageEntry storageEntry) {
        def oldName = params.storageEntry.old_name
        if (oldName != storageEntry.name && storageEntry.exists()) storageEntry.errors.rejectValue('name', 'exists', [storageEntry.name] as Object[], null)

        if (storageEntry.hasErrors()) {
            render view: 'edit', model: [
                    storageEntry: storageEntry
            ]
            return
        }

        storageEntry.configService = configService // TODO: this is probably bad form

        storageEntry.write()

        if (oldName != storageEntry.name) deleteEntry(new StorageEntry([name: oldName, type: storageEntry.type]).xmlKey)

        request.withFormat {
            form multipartForm {
                flash.message = 'storage cluster updated'
                redirect action: 'index', method: 'GET'
            }
            '*' { respond storageEntry, [status: OK] }
        }
    }

    def delete() {
        deleteEntry(params.storageKey)
        redirect action: 'index'
    }

    private deleteEntry(xmlKey) {
        new StorageEntry([xmlKey: xmlKey]).allKeys.each { configService.deleteConfigObject(it) }
    }

    def test(StorageEntry storageEntry) {
        // TODO: abstract the storage cluster types a bit better here
        if (storageEntry.type == 'atmos') {
            render atmosService.testConfig((AtmosStorage) storageEntry.storage) as String
        } else if (storageEntry.type == 'ecs') {
            render ecsService.testConfig((EcsStorage) storageEntry.storage) as String
        }
    }
}

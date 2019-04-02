package sync.ui.storage

import grails.converters.JSON
import sync.ui.ConfigAccessor

class AtmosController implements ConfigAccessor {
    def atmosService

    def show() {
        def storageEntry = new StorageEntry([configService: configService, xmlKey: params.storageKey])
        if (!storageEntry.exists() || storageEntry.type != 'atmos') render status: 404
        [
                tenant: atmosService.getTenantInfo(storageEntry.storage).tenant
        ]
    }

    def subtenant() {
        def storageEntry = new StorageEntry([configService: configService, xmlKey: params.storageKey])
        if (!storageEntry.exists() || storageEntry.type != 'atmos' || !(params.subtenantName)) render status: 404
        [
                subtenant: atmosService.getSubtenantDetails(storageEntry.storage, params.subtenantName).subtenant
        ]
    }

    def getSubtenant() {
        def storageEntry = new StorageEntry([configService: configService, xmlKey: params.storageKey])
        if (!storageEntry.exists() || storageEntry.type != 'atmos' || !(params.subtenantName)) render status: 404
        render atmosService.getSubtenantDetails(storageEntry.storage, params.subtenantName).subtenant as JSON
    }

    def listSubtenants() {
        def storageEntry = new StorageEntry([configService: configService, xmlKey: params.storageKey])
        if (!storageEntry.exists() || storageEntry.type != 'atmos') render status: 404
        render atmosService.getTenantInfo(storageEntry.storage).tenant.subtenantList as JSON
    }
}

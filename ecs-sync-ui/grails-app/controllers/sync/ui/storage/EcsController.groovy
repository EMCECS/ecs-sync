package sync.ui.storage

import grails.converters.JSON
import sync.ui.ConfigAccessor

class EcsController implements ConfigAccessor {
    def ecsService

    def show() {
        def storageEntry = new StorageEntry([configService: configService, xmlKey: params.storageKey])
        if (!storageEntry.exists() || storageEntry.type != 'ecs') render status: 404
        [
                storageEntry: storageEntry,
                namespaces  : ecsService.listNamespaces(storageEntry.storage as EcsStorage)
        ]
    }

    def namespace() {
        def storageEntry = new StorageEntry([configService: configService, xmlKey: params.storageKey])
        if (!storageEntry.exists() || storageEntry.type != 'ecs' || !(params.subtenantName)) render status: 404
        [
                namespace: ecsService.getNamespaceInfo(storageEntry.storage as EcsStorage, params.namespaceId)
        ]
    }

    def listNamespaces() {
        def storageEntry = new StorageEntry([configService: configService, xmlKey: params.storageKey])
        if (!storageEntry.exists() || storageEntry.type != 'ecs') render status: 404
        render ecsService.listNamespaces(storageEntry.storage as EcsStorage) as JSON
    }

    def listReplicationGroups() {
        def storageEntry = new StorageEntry([configService: configService, xmlKey: params.storageKey])
        if (!storageEntry.exists() || storageEntry.type != 'ecs') render status: 404
        render ecsService.listReplicationGroups(storageEntry.storage as EcsStorage) as JSON
    }
}

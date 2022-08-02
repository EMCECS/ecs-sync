/*
 * Copyright (c) 2018-2019 Dell Inc. or its subsidiaries. All Rights Reserved.
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

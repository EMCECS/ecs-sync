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

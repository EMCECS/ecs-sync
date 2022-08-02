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
package sync.ui.migration.a2e

import groovy.util.logging.Slf4j
import sync.ui.migration.TaskStatus
import sync.ui.storage.AtmosService
import sync.ui.storage.AtmosStorage

@Slf4j
class S3ProxyUserMapTask extends AbstractS3ProxyTask {
    @Lazy
    volatile name = "S3 Proxy - Configure mapped users for subtenant ${migration.migrationConfig.atmosSubtenantName}"
    def description = "Creates the user mapping for the S3 Proxy. Once this mapping is set, it cannot change, so " +
            "if the mapping already exists, return success, with a warning."

    @Override
    void execute() {
        // only run if the S3 Proxy is enabled for this migration
        if (migration.migrationConfig.useS3Proxy) {

            AtmosService atmosService = migration.atmosService
            AtmosStorage atmosStorage = migration.sourceStorage as AtmosStorage
            String atmosSubtenant = migration.migrationConfig.atmosSubtenantName

            try {
                // get subtenant ID
                def subtenantId = atmosService.getSubtenantDetails(atmosStorage, atmosSubtenant).subtenant.id
                percentComplete = 33

                // check if user map exists (do not regenerate)
                if (migration.s3ProxyObjectExists(userMapKey(subtenantId))) {
                    taskStatus = TaskStatus.Success
                    taskMessage = "User map already established for subtenant ${subtenantId}"
                } else {
                    percentComplete = 66

                    def mapData = migration.migrationConfig.userMap.collect { k, v -> "${k} ${v}" }.join('\n')

                    // write user map
                    migration.s3ProxyCreateObject(userMapKey(subtenantId), mapData)
                    taskStatus = TaskStatus.Success
                    taskMessage = "User map established for subtenant ${subtenantId}"
                }
            } catch (e) {
                // unexpected error
                log.error("Unexpected error", e)
                taskStatus = TaskStatus.Error
                taskMessage = e.message
            }
        } else {
            taskStatus = TaskStatus.Success
            taskMessage = 'S3 Proxy is not in use'
        }

        percentComplete = 100
    }
}

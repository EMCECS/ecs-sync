/*
 * Copyright 2013-2018 EMC Corporation. All Rights Reserved.
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
class S3ProxySubtenantPreMigrationTask extends AbstractS3ProxyTask {
    @Lazy
    volatile name = "S3 Proxy - Subtenant ${migration.migrationConfig.atmosSubtenantName} in Pre-Migration"
    def description = "Changes status of a subtenant in the S3 Proxy to pre-migration. In this state, all requests " +
            "go to Atmos and no new buckets can be created. The purpose of this state is to provide synchronization " +
            "around listing all of the buckets in the source subtenant. If a subtenant has already been fully migrated, " +
            "it cannot be placed into this state." +
            "<ol>" +
            "<li>Only execute if the S3 proxy is enabled for this migration</li>" +
            "<li>Obtain subtenant ID from name</li>" +
            "<li>If subtenant ID is already in pre-migrating list, return success</li>" +
            "<li>If subtenant ID is already in migrating list, return success</li>" +
            "<li>If subtenant ID is in post-migration list, return an error (cannot re-migrate a subtenant)</li>" +
            "<li>Else, add subtenant ID to pre-migration list</li>" +
            "</ol>"

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
                def s3Client = migration.s3ProxyClient
                percentComplete = 20

                // create object if it does not exist
                if (!migration.s3ProxyObjectExists(subtPreMigKey))
                    migration.s3ProxyCreateObject(subtPreMigKey)

                // check if subtenant ID exists in list
                if (s3Client.readObject(migration.migrationConfig.s3ProxyBucket, subtPreMigKey, String.class) =~ "(?m)^${subtenantId}\$") {
                    taskStatus = TaskStatus.Success
                    taskMessage = "Subtenant ${subtenantId} already in pre-migration list"
                } else {
                    percentComplete = 40

                    // check if subtenant is already in migration (likely split up by users - this is ok)
                    if (migration.s3ProxyObjectExists(subtMigKey)
                            && s3Client.readObject(migration.migrationConfig.s3ProxyBucket, subtMigKey, String.class) =~ "(?m)^${subtenantId}\$") {
                        taskStatus = TaskStatus.Success
                        taskMessage = "Subtenant ${subtenantId} already in migrating list"
                    } else {
                        percentComplete = 60

                        // check if subtenant is already in post-migration (we cannot re-migrate it)
                        if (migration.s3ProxyObjectExists(subtPostMigKey) &&
                                s3Client.readObject(migration.migrationConfig.s3ProxyBucket, subtPostMigKey, String.class) =~ "(?m)^${subtenantId}\$") {
                            taskStatus = TaskStatus.Error
                            taskMessage = "Subtenant ${subtenantId} has already been migrated (cannot re-migrate)"
                        } else {
                            percentComplete = 80

                            // append subtenant ID to list
                            s3Client.appendObject(migration.migrationConfig.s3ProxyBucket, subtPreMigKey, "${subtenantId}\n" as String)
                            taskStatus = TaskStatus.Success
                            taskMessage = "Subtenant ${subtenantId} added to pre-migration list"
                        }
                    }
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

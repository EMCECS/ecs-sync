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

import com.emc.object.s3.bean.CannedAcl
import com.emc.object.s3.request.PutObjectRequest
import groovy.util.logging.Slf4j
import sync.ui.migration.TaskStatus
import sync.ui.storage.AtmosService
import sync.ui.storage.AtmosStorage

@Slf4j
class S3ProxySubtenantMigratingTask extends AbstractS3ProxyTask {
    @Lazy
    volatile name = "S3 Proxy - Subtenant ${migration.migrationConfig.atmosSubtenantName} in Migration"
    def description = "Changes status of a subtenant from pre-migration to in-migration, presumably right before the buckets are copied. " +
            "To change the state of the subtenant, it is added to the post-migration list, and removed from the migrating list. " +
            "<ol>" +
            "<li>Only execute if the S3 proxy is enabled for this migration</li>" +
            "<li>Obtain subtenant ID from name</li>" +
            "<li>If subtenant ID is already in migrating list, return success</li>" +
            "<li>If subtenant ID is not in pre-migration list, return an error (subtenant should have been in pre-migration list)</li>" +
            "<li>Else, add subtenant ID to migrating list, then remove it from pre-migration list</li>" +
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
                if (!migration.s3ProxyObjectExists(subtMigKey))
                    migration.s3ProxyCreateObject(subtMigKey)

                // check if subtenant ID exists in list
                if (s3Client.readObject(migration.migrationConfig.s3ProxyBucket, subtMigKey, String.class) =~ "(?m)^${subtenantId}\$") {
                    taskStatus = TaskStatus.Success
                    taskMessage = "Subtenant ${subtenantId} already in migrating list"
                } else {
                    percentComplete = 40

                    // use ETag for optimistic lock
                    def preMigResponse = s3Client.getObject(migration.migrationConfig.s3ProxyBucket, subtPreMigKey)
                    def etag = preMigResponse.objectMetadata.getETag()
                    def preMigData = preMigResponse.getObject().getText('UTF-8')

                    // make sure the subtenant is in pre-migration list
                    if (!(preMigData =~ "(?m)^${subtenantId}\$")) {
                        taskStatus = TaskStatus.Error
                        taskMessage = "Subtenant ${subtenantId} is not in pre-migration list!"
                    } else {
                        percentComplete = 60

                        // append subtenant ID to migrating list
                        s3Client.appendObject(migration.migrationConfig.s3ProxyBucket, subtMigKey, "${subtenantId}\n" as String)
                        percentComplete = 80

                        // remove subtenant from pre-migration list (use ETag as optimistic lock)
                        preMigData = preMigData.replaceAll(/(?m)^${subtenantId}\n/, '')
                        def removeReq = new PutObjectRequest(migration.migrationConfig.s3ProxyBucket, subtPreMigKey, preMigData)
                        removeReq.withIfMatch(etag).withCannedAcl(CannedAcl.PublicRead)
                        s3Client.putObject(removeReq)

                        taskStatus = TaskStatus.Success
                        taskMessage = "Subtenant ${subtenantId} moved to migrating list"
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

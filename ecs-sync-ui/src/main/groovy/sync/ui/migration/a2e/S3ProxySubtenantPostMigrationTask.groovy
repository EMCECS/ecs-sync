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

import com.emc.object.s3.bean.CannedAcl
import com.emc.object.s3.request.PutObjectRequest
import groovy.util.logging.Slf4j
import sync.ui.migration.TaskStatus
import sync.ui.storage.AtmosService
import sync.ui.storage.AtmosStorage

@Slf4j
class S3ProxySubtenantPostMigrationTask extends AbstractS3ProxyTask {
    @Lazy
    volatile name = "S3 Proxy - Subtenant ${migration.migrationConfig.atmosSubtenantName} in Post-Migration"
    def description = "Changes status of a subtenant from in-migration to post-migration, presumably after all buckets were successfully copied. " +
            "The subtenant pre- and in-migration bucket lists must be empty prior to changing the state of the subtenant. " +
            "To change the state of the subtenant, it is added to the post-migration list, and removed from the migrating list. " +
            "<ol>" +
            "<li>Only execute if the S3 proxy is enabled for this migration</li>" +
            "<li>Obtain subtenant ID from name</li>" +
            "<li>If subtenant ID is already in post-migration list, return success with warning</li>" +
            "<li>If any buckets are present in pre-migration or migrating lists, return an error</li>" +
            "<li>If subtenant ID is in migrating list, add subtenant ID to post-migration list, then remove it from migrating list</li>" +
            "<li>Else, return an error (subtenant should have been in migrating list)</li>" +
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

                // create list object if it does not exist
                if (!migration.s3ProxyObjectExists(subtPostMigKey))
                    migration.s3ProxyCreateObject(subtPostMigKey)
                percentComplete = 40

                // check if subtenant is already in post-migration
                if (s3Client.readObject(migration.migrationConfig.s3ProxyBucket, subtPostMigKey, String.class) =~ "(?m)^${subtenantId}\$") {
                    taskMessage = "WARNING: subtenant ${subtenantId} is already in post-migration"

                    // error if any buckets in pre-migration list
                } else if (migration.s3ProxyObjectExists(bucketsPreMigKey(subtenantId)) &&
                        s3Client.readObject(migration.migrationConfig.s3ProxyBucket, bucketsPreMigKey(subtenantId), String.class).trim()) {
                    taskStatus = TaskStatus.Error
                    taskMessage = "Subtenant ${subtenantId} has buckets in pre-migration state"

                    // error if buckets in migrating list
                } else if (migration.s3ProxyObjectExists(bucketsMigKey(subtenantId)) &&
                        s3Client.readObject(migration.migrationConfig.s3ProxyBucket, bucketsMigKey(subtenantId), String.class).trim()) {
                    taskStatus = TaskStatus.Error
                    taskMessage = "Subtenant ${subtenantId} has buckets in migration state"

                } else {
                    // change subtenant state to post-migration
                    percentComplete = 80

                    // use ETag for optimistic lock
                    def migResponse = s3Client.getObject(migration.migrationConfig.s3ProxyBucket, subtMigKey)
                    def etag = migResponse.objectMetadata.getETag()
                    def migData = migResponse.getObject().getText('UTF-8')
                    percentComplete = 90

                    // check if subtenant exists in migrating list
                    if (migData =~ "(?m)^${subtenantId}\$") {

                        // append subtenant ID to post-migration list
                        s3Client.appendObject(migration.migrationConfig.s3ProxyBucket, subtPostMigKey, "${subtenantId}\n" as String)
                        percentComplete = 95

                        // remove subtenant from migrating list (use ETag as optimistic lock)
                        migData = migData.replaceAll(/(?m)^${subtenantId}\n/, '')
                        def removeReq = new PutObjectRequest(migration.migrationConfig.s3ProxyBucket, subtMigKey, migData)
                        removeReq.withIfMatch(etag).withCannedAcl(CannedAcl.PublicRead)
                        s3Client.putObject(removeReq)

                        taskStatus = TaskStatus.Success
                        taskMessage = "Subtenant ${subtenantId} moved to post-migration list"
                    } else {
                        taskStatus = TaskStatus.Error
                        taskMessage = "Subtenant ${subtenantId} was not in migrating list"
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

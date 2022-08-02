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
class S3ProxyBucketMigratingTask extends AbstractS3ProxyTask {
    def bucket

    @Lazy
    volatile name = "S3 Proxy - Bucket ${migration.migrationConfig.atmosSubtenantName}/${bucket} in Migration"
    @Lazy
    volatile description = "Changes status of a bucket from pre-migration to in-migration in the S3 Proxy, presumably right before the data is copied. " +
            "This will first add the bucket to the migrating list (${bucketsMigKey('subtenant-id')}), " +
            "then remove it from the pre-migration list for the subtenant (${bucketsPreMigKey('subtenant-id')}). " +
            "<ol>" +
            "<li>Only execute if the S3 proxy is enabled for this migration</li>" +
            "<li>Obtain subtenant ID from name</li>" +
            "<li>If bucket is in migrating list, do nothing</li>" +
            "<li>Else, if bucket is in pre-migration list, add it to migrating list and remove it from pre-migration list</li>" +
            "<li>Otherwise, return with error (bucket should have been in one of those two lists)</li>" +
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
                def listKey = bucketsMigKey(subtenantId)
                def s3Client = migration.s3ProxyClient
                percentComplete = 20

                // create object if it does not exist
                if (!migration.s3ProxyObjectExists(listKey))
                    migration.s3ProxyCreateObject(listKey)

                // check if bucket exists in list
                if (s3Client.readObject(migration.migrationConfig.s3ProxyBucket, listKey, String.class) =~ "(?m)^${bucket}\$") {
                    taskStatus = TaskStatus.Success
                    taskMessage = "Bucket ${subtenantId}/${bucket} already in migrating list"
                } else {
                    percentComplete = 40

                    // use ETag for optimistic lock
                    def preMigResponse = s3Client.getObject(migration.migrationConfig.s3ProxyBucket, bucketsPreMigKey(subtenantId))
                    def etag = preMigResponse.objectMetadata.getETag()
                    def preMigData = preMigResponse.getObject().getText('UTF-8')

                    // make sure the bucket is in pre-migration list
                    if (!(preMigData =~ "(?m)^${bucket}\$")) {
                        taskStatus = TaskStatus.Error
                        taskMessage = "Bucket ${subtenantId}/${bucket} is not in pre-migration list!"
                    } else {
                        percentComplete = 60

                        // append bucket to migrating list
                        s3Client.appendObject(migration.migrationConfig.s3ProxyBucket, listKey, "${bucket}\n" as String)
                        percentComplete = 80

                        // remove bucket from pre-migration list (use ETag as optimistic lock)
                        preMigData = preMigData.replaceAll(/(?m)^${bucket}\n/, '')
                        def removeReq = new PutObjectRequest(migration.migrationConfig.s3ProxyBucket, bucketsPreMigKey(subtenantId), preMigData)
                        removeReq.withIfMatch(etag).withCannedAcl(CannedAcl.PublicRead)
                        s3Client.putObject(removeReq)

                        taskStatus = TaskStatus.Success
                        taskMessage = "Bucket ${subtenantId}/${bucket} moved to migrating list"
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

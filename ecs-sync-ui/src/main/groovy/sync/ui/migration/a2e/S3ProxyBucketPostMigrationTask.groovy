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
class S3ProxyBucketPostMigrationTask extends AbstractS3ProxyTask {
    def bucket

    @Lazy
    volatile name = "S3 Proxy - Bucket ${migration.migrationConfig.atmosSubtenantName}/${bucket} in Post-Migration"
    @Lazy
    volatile description = "Changes status of a bucket from in-migration to post-migration, presumably after all data was successfully copied. " +
            "There is no list for post-migration buckets; this is the default status for any buckets not in a list. Therefore, " +
            "this will simply remove the bucket from the migrating list for the subtenant (${bucketsMigKey('subtenant-id')}). " +
            "<ol>" +
            "<li>Only execute if the S3 proxy is enabled for this migration</li>" +
            "<li>Obtain subtenant ID from name</li>" +
            "<li>If bucket is in migrating list, remove it</li>" +
            "<li>Else, return with warning (bucket should have been in list)</li>" +
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
                def previousListKey = bucketsMigKey(subtenantId)
                def s3Client = migration.s3ProxyClient
                percentComplete = 33

                // use ETag for optimistic lock
                def migResponse = s3Client.getObject(migration.migrationConfig.s3ProxyBucket, previousListKey)
                def etag = migResponse.objectMetadata.getETag()
                def migData = migResponse.getObject().getText('UTF-8')
                percentComplete = 66

                // check if bucket exists in migrating list
                if (migData =~ "(?m)^${bucket}\$") {

                    // remove bucket from migrating list (use ETag as optimistic lock)
                    migData = migData.replaceAll(/(?m)^${bucket}\n/, '')
                    def removeReq = new PutObjectRequest(migration.migrationConfig.s3ProxyBucket, previousListKey, migData)
                    removeReq.withIfMatch(etag).withCannedAcl(CannedAcl.PublicRead)
                    s3Client.putObject(removeReq)

                    taskStatus = TaskStatus.Success
                    taskMessage = "Bucket ${subtenantId}/${bucket} removed from migrating list"
                } else {
                    taskStatus = TaskStatus.Success
                    taskMessage = "WARNING: Bucket ${subtenantId}/${bucket} was not in migrating list"
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

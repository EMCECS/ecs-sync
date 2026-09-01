/*
 * Copyright (c) 2024 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package sync.ui.migration.e2e

import com.emc.ecs.sync.cluster.EcsBucketInfo
import com.emc.ecs.sync.config.Protocol
import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.config.SyncOptions
import com.emc.ecs.sync.config.storage.EcsS3Config
import com.emc.ecs.sync.rest.JobControlStatus
import groovy.util.logging.Slf4j
import sync.ui.SyncUtil
import sync.ui.migration.AbstractMigrationTask
import sync.ui.migration.TaskStatus
import sync.ui.storage.EcsStorage

import java.security.MessageDigest

/**
 * Submits one ecs-sync job to copy a single bucket, then polls it to completion. Using the normal job service means
 * the copy shows up in the UI's job list with full progress, pause/resume and reporting.
 */
@Slf4j
class BucketSyncTask extends AbstractMigrationTask<E2EMigration> {
    String sourceNamespace
    EcsBucketInfo bucket
    def jobId

    @Lazy
    volatile name = "ecs-sync: migrating ${sourceNamespace}/${bucket.name} to " +
            "${migration.targetNamespaceFor(sourceNamespace)}/${bucket.name}"
    def description = "Submits a sync job to copy one bucket from the source ECS to the target ECS.<br>" +
            "Success is determined by the following checks:<ul>" +
            "<li>Job status is Complete (not Failed or Stopped)</li>" +
            "<li>No general error message</li>" +
            "<li>No object errors</li>" +
            "<li>Objects completed + objects skipped == objects expected</li></ul>"

    @Override
    void execute() {
        try {
            def syncConfig = createSyncConfig()

            def result = migration.syncJobService.submitJob(syncConfig)
            jobId = result.jobId

            if (!result.success) {
                taskStatus = TaskStatus.Error
                taskMessage = "Failed to submit sync job (${result.statusCode}: ${result.message})"
                return
            }

            // wait until the sync is finished
            def progress
            while (true) {
                progress = migration.syncJobService.getJobStatus(result.jobId)
                percentComplete = (SyncUtil.calculateProgress(progress) * 100).intValue()
                if (progress.status.finalState) break
                sleep(1000)
            }

            if (progress.status == JobControlStatus.Complete && !progress.runError && !progress.objectsFailed
                    && (progress.objectsComplete + progress.objectsSkipped == progress.totalObjectsExpected)) {

                // archive the job to generate a report, then remove it from the sync service
                migration.historyService.archiveJob(result.jobId)
                migration.syncJobService.deleteJob(result.jobId, true)

                taskMessage = "Copied ${progress.objectsComplete} objects " +
                        "(${progress.objectsSkipped} skipped, ${progress.totalObjectsExpected} expected)"
                taskStatus = TaskStatus.Success
            } else {
                taskStatus = TaskStatus.Error
                def message = progress.runError ?: "${progress.totalObjectsExpected} expected, " +
                        "${progress.objectsComplete} successful, ${progress.objectsSkipped} skipped, " +
                        "${progress.objectsFailed} errors"
                taskMessage = "Bucket copy did not complete successfully (${progress.status} - ${message})"
            }
        } catch (e) {
            log.error("Unexpected error copying bucket ${bucket.name}", e)
            taskStatus = TaskStatus.Error
            taskMessage = e.message
        }
    }

    private SyncConfig createSyncConfig() {
        def sourceStorage = migration.sourceStorage as EcsStorage
        def targetStorage = migration.targetStorage as EcsStorage
        def sourceEndpoint = new URI(sourceStorage.s3ApiEndpoint)
        def targetEndpoint = new URI(targetStorage.s3ApiEndpoint)

        def targetNamespace = migration.targetNamespaceFor(sourceNamespace)
        def sourceUser = migration.resolveBucketUser(sourceNamespace, bucket)
        if (!sourceUser) throw new IllegalStateException("namespace ${sourceNamespace} has no object users")
        def targetUser = migration.targetUserFor(sourceNamespace, sourceUser)

        // secret keys are per-cluster AND per-namespace - a user's key on the source is unrelated to the key of the
        // corresponding user on the target
        def sourceKey = migration.sourceClient.getOrCreateSecretKey(sourceUser, sourceNamespace,
                migration.migrationConfig.generateSourceSecretKeys)
        def targetKey = migration.targetClient.getOrCreateSecretKey(targetUser, targetNamespace, true)

        // job name algorithm mirrors the a2e migration:
        // {first-20-of-namespace}_{first-20-of-bucket}_{first-4-of-task-name-md5}{first-4-of-migration-guid-md5}
        def nhash = MessageDigest.getInstance("MD5").digest(name.bytes).encodeHex().toString()
        def ghash = MessageDigest.getInstance("MD5").digest(migration.migrationConfig.guid.bytes).encodeHex().toString()
        String jobName = "${targetNamespace.take(20)}_${bucket.name.take(20)}_${nhash.take(4)}${ghash.take(4)}"

        new SyncConfig([
                jobName   : jobName,
                source    : new EcsS3Config([
                        protocol           : Protocol.valueOf(sourceEndpoint.scheme.toLowerCase()),
                        host               : sourceEndpoint.host,
                        port               : sourceEndpoint.port,
                        // pin the namespace explicitly: for a same-cluster migration the source and target bucket
                        // names are identical, so without it both sides could resolve to the same namespace
                        namespace          : sourceNamespace,
                        accessKey          : sourceUser,
                        secretKey          : sourceKey,
                        smartClientEnabled : sourceStorage.s3SmartClient,
                        bucketName         : bucket.name,
                        includeVersions    : migration.migrationConfig.includeVersions && bucket.versioningEnabled,
                        apacheClientEnabled: true
                ]),
                target    : new EcsS3Config([
                        protocol           : Protocol.valueOf(targetEndpoint.scheme.toLowerCase()),
                        host               : targetEndpoint.host,
                        port               : targetEndpoint.port,
                        namespace          : targetNamespace,
                        accessKey          : targetUser,
                        secretKey          : targetKey,
                        smartClientEnabled : targetStorage.s3SmartClient,
                        bucketName         : bucket.name,
                        createBucket       : false, // created by CreateBucketTask, with the correct owner and RG
                        includeVersions    : migration.migrationConfig.includeVersions && bucket.versioningEnabled,
                        apacheClientEnabled: true
                ]),
                options   : new SyncOptions([
                        syncAcl    : true,
                        syncMetadata: true,
                        verify     : migration.migrationConfig.verify,
                        threadCount: migration.migrationConfig.threadCount,
                        dbTable    : SyncUtil.conformDbTable(jobName)
                ]),
                properties: [
                        migrationGuid: migration.migrationConfig.guid as String,
                        migrationDesc: migration.migrationConfig.description as String,
                        taskName     : name as String
                ]
        ])
    }
}

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
package sync.ui.migration.a2e

import com.emc.ecs.sync.config.Protocol
import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.config.SyncOptions
import com.emc.ecs.sync.config.filter.AclMappingConfig
import com.emc.ecs.sync.config.storage.AwsS3Config
import com.emc.ecs.sync.config.storage.EcsS3Config
import com.emc.ecs.sync.rest.JobControlStatus
import groovy.util.logging.Slf4j
import sync.ui.SyncUtil
import sync.ui.migration.TaskStatus
import sync.ui.storage.AtmosStorage
import sync.ui.storage.EcsStorage

import java.security.MessageDigest

@Slf4j
class S3SyncTask extends AbstractS3ProxyTask {
    @Lazy
    volatile name = "ecs-sync: migrating ${migration.migrationConfig.atmosSubtenantName}/${bucket} (as ${atmosUser}) " +
            "to ${migration.migrationConfig.userNamespaceMap[atmosUser]}/${bucket} (as ${ecsUser})"
    def description = "Submits a sync job to migrate one S3 bucket from Atmos to ECS.<br>" +
            "Success is determined by the following checks:<ul>" +
            "<li>Job status is Complete (not Failed or Stopped)</li>" +
            "<li>No general error message</li>" +
            "<li>No object errors</li>" +
            "<li>Objects completed + objects skipped == objects expected</li></ul>"

    def atmosUser
    def bucket
    def jobId

    @Lazy
    volatile ecsUser = migration.migrationConfig.userMap[atmosUser]

    @Lazy
    volatile syncConfig = createSyncConfig()

    @Override
    void execute() {
        try {

            // check if the bucket is in migrating list (we should not migrate unless it is)
            if (migration.migrationConfig.useS3Proxy) {
                def subtenantId = migration.atmosSubtenant.id
                if (!(migration.s3ProxyClient.readObject(migration.migrationConfig.s3ProxyBucket, bucketsMigKey(subtenantId), String.class) =~ "(?m)^${bucket}\$")) {
                    percentComplete = 100
                    taskStatus = TaskStatus.Error
                    taskMessage = "Bucket ${subtenantId}/${bucket} is not in migrating list (has it already been migrated?)"
                    return
                }
            }

            def result = migration.syncJobService.submitJob(syncConfig)
            jobId = result.jobId

            if (result.success) {
                // wait until the sync is finished
                def progress
                while (true) {
                    progress = migration.syncJobService.getJobStatus(result.jobId)

                    percentComplete = (SyncUtil.calculateProgress(progress) * 100).intValue()

                    if (progress.status.finalState) break

                    sleep(1000)
                }

                // job is done; check status
                // to determine success, we are checking that:
                // - status is Complete (not Failed or Stopped)
                // - no general error message
                // - no object errors
                // - objects completed + skipped == objects expected
                if (progress.status == JobControlStatus.Complete && !progress.runError && !progress.objectsFailed
                        && (progress.objectsComplete + progress.objectsSkipped == progress.totalObjectsExpected)) {

                    // archive the job to generate a report
                    migration.historyService.archiveJob(result.jobId)
                    // delete job from sync service (always keep database for auditing)
                    migration.syncJobService.deleteJob(result.jobId, true)

                    // check expected vs actual (only warn if off)
                    if (progress.objectsComplete + progress.objectsSkipped == progress.totalObjectsExpected) {
                        taskMessage = "ecs-sync migration was successful (${progress.objectsComplete} complete + ${progress.objectsSkipped} skipped = ${progress.totalObjectsExpected} expected)"
                    } else {
                        taskMessage = "WARNING: ecs-sync migration completed, but count is off (${progress.totalObjectsExpected} expected != ${progress.objectsComplete} copied + ${progress.objectsSkipped} skipped)"
                    }
                    taskStatus = TaskStatus.Success
                } else {
                    taskStatus = TaskStatus.Error
                    def message = progress.runError ?: "${progress.totalObjectsExpected} expected, ${progress.objectsComplete} successful, ${progress.objectsSkipped} skipped, ${progress.objectsFailed} errors"
                    taskMessage = "ecs-sync migration did not complete successfully (${progress.status} - ${message})"
                }
            } else {
                taskStatus = TaskStatus.Error
                taskMessage = "Failed to submit sync job (${result.statusCode}: ${result.message})"
            }
        } catch (e) {
            // unexpected error
            log.error("Unexpected error", e)
            taskStatus = TaskStatus.Error
            taskMessage = e.message
        }
    }

    private createSyncConfig() {
        // collect source details
        def atmosStorage = migration.sourceStorage as AtmosStorage
        def sourceEndpoint = atmosStorage.s3ApiEndpoint.toURI()
        def subtenant = migration.atmosSubtenant
        String sourceUser = "${subtenant.id}/${atmosUser}"
        String sourceKey = subtenant.objectUsers.find { it.uid == atmosUser }.sharedSecret

        // collect target details
        def ecsStorage = migration.targetStorage as EcsStorage
        def targetEndpoint = ecsStorage.s3ApiEndpoint.toURI()
        String targetUser = ecsUser
        String ecsNamespace = migration.migrationConfig.userNamespaceMap[atmosUser]
        String targetKey = migration.ecsService.getSecretKey(ecsStorage, targetUser, ecsNamespace).secretKey1

        // job name algorithm: {first-20-of-subtenant}_{first-20-of-bucket}_{first-4-of-task-name-md5}{first-4-of-migration-guid-md5}
        def subt = migration.migrationConfig.atmosSubtenantName
        def nhash = MessageDigest.getInstance("MD5").digest(name.bytes).encodeHex().toString()
        def ghash = MessageDigest.getInstance("MD5").digest(migration.migrationConfig.guid.bytes).encodeHex().toString()
        String jobName = "${subt.take(20)}_${bucket.take(20)}_${nhash.take(4)}${ghash.take(4)}"

        // TODO: enumerate node IPs for data access?
        new SyncConfig([
                jobName   : jobName,
                source    : new AwsS3Config([
                        protocol        : Protocol.valueOf(sourceEndpoint.scheme.toLowerCase()),
                        host            : sourceEndpoint.host,
                        port            : sourceEndpoint.port,
                        accessKey       : sourceUser,
                        secretKey       : sourceKey,
                        disableVHosts   : true,
                        bucketName      : bucket,
                        legacySignatures: true,
                        urlDecodeKeys   : false, // Atmos does not encode the keys (invalid XML characters will cause errors!)
                        // Atmos stores MPUs inside the bucket
                        excludedKeys    : ['\\.multipart_uploads/.*']
                ]),
                filters   : [
                        new AclMappingConfig([
                                aclMapInstructions:
                                        migration.migrationConfig.userMap.collect { sUser, tUser ->
                                            "user.${sUser}=${tUser}\n"
                                        }.join('') as String
                        ])
                ],
                target    : new EcsS3Config([
                        protocol           : Protocol.valueOf(targetEndpoint.scheme.toLowerCase()),
                        host               : targetEndpoint.host,
                        port               : targetEndpoint.port,
                        accessKey          : targetUser,
                        secretKey          : targetKey,
                        smartClientEnabled : ecsStorage.s3SmartClient,
                        bucketName         : bucket,
                        createBucket       : true,
                        apacheClientEnabled: true
                ]),
                options   : new SyncOptions([
                        syncAcl    : true,
                        verify     : true,
                        threadCount: 20,
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

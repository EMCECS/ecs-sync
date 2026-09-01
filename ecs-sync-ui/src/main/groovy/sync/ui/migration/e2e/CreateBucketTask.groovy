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
import groovy.util.logging.Slf4j
import sync.ui.migration.AbstractMigrationTask
import sync.ui.migration.TaskStatus

@Slf4j
class CreateBucketTask extends AbstractMigrationTask<E2EMigration> {
    String sourceNamespace
    EcsBucketInfo bucket

    @Lazy
    volatile name = "Create bucket ${migration.targetNamespaceFor(sourceNamespace)}/${bucket.name}"
    def description = "Creates the bucket on the target in the selected replication group, owned by the replicated " +
            "object user so that ownership matches the source."

    @Override
    void execute() {
        def targetNamespace = migration.targetNamespaceFor(sourceNamespace)
        try {
            if (migration.targetClient.getBucketInfo(bucket.name, targetNamespace) != null) {
                taskMessage = "Bucket ${bucket.name} already exists in ${targetNamespace}"
                taskStatus = TaskStatus.Success
                percentComplete = 100
                return
            }

            def sourceUser = migration.resolveBucketUser(sourceNamespace, bucket)
            if (!sourceUser) {
                taskMessage = "Namespace ${sourceNamespace} has no object users, so the bucket cannot be owned"
                taskStatus = TaskStatus.Error
                percentComplete = 100
                return
            }
            def owner = migration.targetUserFor(sourceNamespace, sourceUser)
            percentComplete = 50

            migration.targetClient.createBucket(bucket.name, targetNamespace,
                    migration.groupResolver.resolve(bucket.replicationGroupName), owner, bucket.fileSystemEnabled)

            taskMessage = "Created bucket ${targetNamespace}/${bucket.name} owned by ${owner}"
            taskStatus = TaskStatus.Success
        } catch (e) {
            log.error("Failed to create bucket ${bucket.name}", e)
            taskMessage = "Failed to create bucket: ${e.message}"
            taskStatus = TaskStatus.Error
        }
        percentComplete = 100
    }
}

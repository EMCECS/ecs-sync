/*
 * Copyright (c) 2019 Dell Inc. or its subsidiaries. All Rights Reserved.
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
import sync.ui.migration.AbstractMigrationTask
import sync.ui.migration.TaskStatus

@Slf4j
class CreateBucketTask extends AbstractMigrationTask<A2EMigration> {
    String atmosUser
    String bucket

    @Lazy
    volatile name = "Create bucket ${bucket} for user ${ecsUser}"
    def description = "Creates the specified bucket with default options if it does not exist."

    @Lazy
    volatile ecsUser = migration.migrationConfig.userMap[atmosUser]

    @Override
    void execute() {
        try {
            def s3Client = migration.targetUserS3Client(atmosUser)

            if (!s3Client.bucketExists(bucket)) {
                s3Client.createBucket(bucket)
                taskMessage = "Created bucket ${bucket} as user ${ecsUser}"
            } else {
                taskMessage = "Bucket ${bucket} already exists"
            }
            taskStatus = TaskStatus.Success
        } catch (e) {
            // unexpected error
            log.error("Unexpected error", e)
            taskStatus = TaskStatus.Error
            taskMessage = e.message
        }

        percentComplete = 100
    }
}

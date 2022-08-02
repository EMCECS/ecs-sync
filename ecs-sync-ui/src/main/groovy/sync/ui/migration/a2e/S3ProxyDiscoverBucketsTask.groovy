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
class S3ProxyDiscoverBucketsTask extends AbstractS3ProxyTask {
    @Lazy
    volatile name = "S3 Proxy - Enumerate buckets in subtenant ${migration.migrationConfig.atmosSubtenantName}"
    @Lazy
    volatile description = "This will enumerate all of the S3 buckets in the Atmos subtenant. " +
            "In Atmos, S3 buckets are in the namespace under the \"/s3/\" directory, so we will simply list that directory " +
            "in the Atmos namespace as any active user, since all authenticated users should have read access. The list is stored " +
            "in the orchestration key ${bucketsPreMigKey('subtenant-id')}"

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
                percentComplete = 10

                // check if bucket list exists (do not regenerate)
                if (migration.s3ProxyObjectExists(bucketsPreMigKey(subtenantId))) {
                    taskStatus = TaskStatus.Success
                    taskMessage = "Buckets already enumerated for subtenant ${subtenantId}"
                } else {
                    percentComplete = 20

                    // enumerate buckets
                    def buckets = atmosService.listAllBuckets(atmosStorage, atmosSubtenant)
                    percentComplete = 90

                    // save bucket pre-migration list
                    migration.s3ProxyCreateObject(bucketsPreMigKey(subtenantId), "${buckets.join('\n')}\n" as String)
                    taskStatus = TaskStatus.Success
                    taskMessage = "Enumerated all buckets for subtenant ${subtenantId}"
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

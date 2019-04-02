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

import com.dellemc.ecs.mgmt.exceptions.EcsException
import grails.util.GrailsUtil
import groovy.util.logging.Slf4j
import sync.ui.migration.AbstractMigrationTask
import sync.ui.migration.TaskStatus
import sync.ui.storage.EcsService
import sync.ui.storage.EcsStorage

@Slf4j
class CreateNamespaceTask extends AbstractMigrationTask<A2EMigration> {
    String namespaceName

    @Lazy
    volatile name = "Create namespace ${namespaceName}"
    def description = "Creates the namespace, if it does not exist, with the specified default replication group"

    @Override
    void execute() {
        EcsStorage ecsStorage = migration.targetStorage as EcsStorage
        EcsService ecsService = migration.ecsService
        String replicationGroup = migration.migrationConfig.replicationGroup

        // check if namespace exists
        def namespace = null
        try {
            namespace = ecsService.getNamespaceInfo(ecsStorage, namespaceName)

            // namespace exists
            taskMessage = "Namespace ${namespaceName} already exists"
        } catch (e) {
            e = GrailsUtil.extractRootCause(e)
            if (e instanceof EcsException && e.errorCode == '1004') {

                // create namespace
                try {
                    namespace = ecsService.createNamespace(ecsStorage, namespaceName, replicationGroup)
                    taskMessage = "Created namespace ${namespaceName}"
                } catch (e2) {
                    // failed to create NS
                    log.error("Failed to create namespace ${namespaceName}", e2)
                    taskMessage = "Failed to create namespace: ${e2.message}"
                }
            } else {
                // unexpected error
                log.error("Unexpected error", e)
                taskMessage = e.message
            }
        }

        percentComplete = 100
        taskStatus = namespace ? TaskStatus.Success : TaskStatus.Error
    }
}

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

import groovy.util.logging.Slf4j
import sync.ui.migration.AbstractMigrationTask
import sync.ui.migration.TaskStatus

@Slf4j
class CreateNamespaceTask extends AbstractMigrationTask<E2EMigration> {
    String sourceNamespace

    @Lazy
    volatile name = {
        def target = migration.targetNamespaceFor(sourceNamespace)
        target == sourceNamespace ? "Create namespace ${target}" : "Create namespace ${target} (from ${sourceNamespace})"
    }()
    def description = "Creates the namespace on the target with the selected replication group, if it does not " +
            "already exist. If the source name is already taken on the target, a suffixed name is used instead."

    @Override
    void execute() {
        def targetNamespace = migration.targetNamespaceFor(sourceNamespace)
        try {
            if (migration.targetClient.getNamespace(targetNamespace) != null) {
                taskMessage = "Namespace ${targetNamespace} already exists"
            } else {
                migration.targetClient.createNamespace(targetNamespace, migration.groupResolver.resolve(null))
                taskMessage = "Created namespace ${targetNamespace}"
            }
            taskStatus = TaskStatus.Success
        } catch (e) {
            log.error("Failed to create namespace ${targetNamespace}", e)
            taskMessage = "Failed to create namespace: ${e.message}"
            taskStatus = TaskStatus.Error
        }
        percentComplete = 100
    }
}

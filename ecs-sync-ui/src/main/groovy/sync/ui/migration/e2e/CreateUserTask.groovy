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
class CreateUserTask extends AbstractMigrationTask<E2EMigration> {
    String sourceNamespace
    String sourceUserId

    @Lazy
    volatile name = {
        def targetUser = migration.targetUserFor(sourceNamespace, sourceUserId)
        targetUser == sourceUserId ? "Create user ${targetUser}" : "Create user ${targetUser} (from ${sourceUserId})"
    }()
    def description = "Creates the object user on the target, if it does not already exist. When the namespace was " +
            "renamed the user is renamed with the same suffix, because ECS may be configured with GLOBAL user scope " +
            "where object user IDs must be unique across the whole cluster."

    @Override
    void execute() {
        def targetNamespace = migration.targetNamespaceFor(sourceNamespace)
        def targetUserId = migration.targetUserFor(sourceNamespace, sourceUserId)
        try {
            def existing = migration.targetClient.listObjectUsers(targetNamespace).collect { it.userId }
            percentComplete = 50

            if (existing.contains(targetUserId)) {
                taskMessage = "User ${targetUserId} already exists in ${targetNamespace}"
            } else {
                migration.targetClient.createObjectUser(targetUserId, targetNamespace)
                taskMessage = "Created user ${targetUserId} in ${targetNamespace}"
            }
            taskStatus = TaskStatus.Success
        } catch (e) {
            log.error("Failed to create user ${targetUserId}", e)
            taskMessage = "Failed to create user: ${e.message}"
            taskStatus = TaskStatus.Error
        }
        percentComplete = 100
    }
}

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

import com.dellemc.ecs.mgmt.exceptions.EcsException
import grails.util.GrailsUtil
import groovy.util.logging.Slf4j
import sync.ui.migration.AbstractMigrationTask
import sync.ui.migration.TaskStatus
import sync.ui.storage.AtmosService
import sync.ui.storage.AtmosStorage
import sync.ui.storage.EcsService
import sync.ui.storage.EcsStorage

@Slf4j
class CreateUserTask extends AbstractMigrationTask<A2EMigration> {
    @Lazy
    volatile name = "Create user ${ecsUser}"
    def description = "<ol>" +
            "<li>If user exists in target and secret keys match, success</li>" +
            "<li>Else, if user exists and keys don't match, display a warning, but continue</li>" +
            "<li>Otherwise, create the mapped user in ECS with the same secret key as in Atmos</li>" +
            "</ol>"

    String atmosUser
    String ecsUser

    @Override
    void execute() {
        AtmosStorage atmosStorage = migration.sourceStorage as AtmosStorage
        AtmosService atmosService = migration.atmosService
        String atmosSubtenant = migration.migrationConfig.atmosSubtenantName
        EcsStorage ecsStorage = migration.targetStorage as EcsStorage
        EcsService ecsService = migration.ecsService
        String ecsNamespace = migration.migrationConfig.userNamespaceMap[atmosUser]

        // check if user exists
        def user = null
        try {
            user = ecsService.getObjectUserInfo(ecsStorage, ecsUser, ecsNamespace)
            percentComplete = 25

            // user exists
            // compare secret keys
            def atmosKey = atmosService.getUserSecretKey(atmosStorage, atmosSubtenant, atmosUser).sharedSecret
            percentComplete = 50
            def keyResp = ecsService.getSecretKey(ecsStorage, ecsUser, ecsNamespace)
            percentComplete = 75

            if (atmosKey in [keyResp.secretKey1, keyResp.secretKey2]) {
                taskMessage = "User ${ecsUser} already exists and secret key matches"
            } else {
                // if secret key doesn't match in ECS, display a warning (but do not fail the task)
                taskMessage = "WARNING: user ${ecsUser} already exists, but secret key does not match"
            }
        } catch (e) {
            e = GrailsUtil.extractRootCause(e)
            if (e instanceof EcsException && e.errorCode == '2000') {
                percentComplete = 25

                try {
                    // get Atmos secret key
                    def secretKey = atmosService.getUserSecretKey(atmosStorage, atmosSubtenant, atmosUser).sharedSecret
                    percentComplete = 50

                    // create user
                    user = ecsService.createObjectUser(ecsStorage, ecsUser, ecsNamespace)
                    percentComplete = 75

                    // set secret key in ECS
                    ecsService.setSecretKey(ecsStorage, ecsUser, ecsNamespace, secretKey)

                    taskMessage = "Created user ${ecsUser}"
                } catch (e2) {
                    // failed to create user
                    log.error("Failed to create user ${ecsUser}", e2)
                    taskMessage = "Failed to create username: ${e2.message}"
                }
            } else {
                // unexpected error
                log.error("Unexpected error", e)
                taskMessage = e.message
            }
        }

        percentComplete = 100
        taskStatus = user ? TaskStatus.Success : TaskStatus.Error
    }
}

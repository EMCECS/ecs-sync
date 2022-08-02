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
package sync.ui

import sync.ui.migration.AbstractMigration
import sync.ui.migration.AbstractMigrationConfig
import sync.ui.migration.a2e.A2EMigration
import sync.ui.migration.a2e.A2EMigrationConfig

class MigrationService implements ConfigAccessor {
    def maxMigrationJobs = 4
    def migrationMap = [:]

    def syncJobService
    def historyService
    def atmosService
    def ecsService

    def listActive() {
        migrationMap.values()
    }

    def testConfig(AbstractMigrationConfig migrationConfig) {
        createMigration(migrationConfig).testConfig()
    }

    def start(AbstractMigrationConfig migrationConfig) {
        if (migrationMap.size() > maxMigrationJobs)
            throw new UnsupportedOperationException("You have reached the maximum number of concurrent migrations (${maxMigrationJobs}).. " +
                    "archive an active migration to start a new one")

        if (migrationMap.containsKey(migrationConfig.guid))
            throw new IllegalStateException("A migration with GUID ${migrationConfig.guid} is already active")

        def migration = createMigration(migrationConfig)

        def migrationThread = new Thread(migration, 'migration-thread')
        migrationThread.setDaemon(true)
        migrationThread.start()

        migrationMap.put(migration.migrationConfig.guid, migration)
    }

    def lookup(guid, required = true) {
        def migration = migrationMap.get(guid)
        if (!migration && required) throw new NoSuchElementException()
        migration
    }

    def pause(guid) {
        lookup(guid).pause()
    }

    def resume(guid) {
        lookup(guid).resume()
    }

    def stop(guid) {
        lookup(guid).stop()
    }

    /**
     * Be sure to save a completion report before deleting!
     */
    def delete(guid) {
        if (!lookup(guid).isStopped()) throw new IllegalStateException("Migrations cannot be deleted while running (you may need to wait for the current task to complete)")
        migrationMap.remove(guid)
    }

    def createMigration(AbstractMigrationConfig migrationConfig) {
        AbstractMigration migration

        // TODO: abstract this better
        if (migrationConfig instanceof A2EMigrationConfig) {
            migration = new A2EMigration([migrationConfig: migrationConfig])
            migration.atmosService = atmosService
            migration.ecsService = ecsService
        } else throw new RuntimeException("Unknown migration config type: ${migrationConfig.class}")

        migration.configService = configService
        migration.syncJobService = syncJobService
        migration.historyService = historyService

        migration
    }
}

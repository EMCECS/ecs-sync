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
import com.emc.ecs.sync.cluster.EcsManagementClient
import com.emc.ecs.sync.cluster.EcsNamespace
import com.emc.ecs.sync.cluster.EcsObjectUser
import com.emc.ecs.sync.cluster.NamespaceMapper
import com.emc.ecs.sync.cluster.ReplicationGroupResolver
import groovy.util.logging.Slf4j
import sync.ui.migration.AbstractMigration
import sync.ui.migration.AbstractMigrationTask
import sync.ui.storage.EcsStorage

/**
 * ECS-to-ECS migration. Discovers the selected namespaces on the source, works out the target names, then creates
 * namespaces, object users and buckets before submitting one ecs-sync job per bucket.
 * <p>
 * The management API work and the naming rules are delegated to the shared {@code ecs-cluster-mgmt} module, so this
 * behaves identically to the {@code EcsClusterSyncCli} command-line tool.
 */
@Slf4j
class E2EMigration extends AbstractMigration<E2EMigrationConfig> {

    private EcsManagementClient sourceClientRef
    private EcsManagementClient targetClientRef

    synchronized EcsManagementClient getSourceClient() {
        if (!sourceClientRef) sourceClientRef = new EcsManagementClient(
                ClusterEndpoints.from(sourceStorage as EcsStorage, migrationConfig.trustSelfSignedCertificates),
                'source')
        sourceClientRef
    }

    synchronized EcsManagementClient getTargetClient() {
        if (!targetClientRef) targetClientRef = new EcsManagementClient(
                ClusterEndpoints.from(targetStorage as EcsStorage, migrationConfig.trustSelfSignedCertificates),
                'target')
        targetClientRef
    }

    /** Releases the management API clients. Safe to call when they were never created. */
    synchronized void closeClients() {
        [sourceClientRef, targetClientRef].each {
            try {
                it?.close()
            } catch (e) {
                log.debug("could not close management client", e)
            }
        }
        sourceClientRef = targetClientRef = null
    }

    @Override
    void run() {
        try {
            super.run()
        } finally {
            closeClients()
        }
    }

    /** Discovered source structure, keyed by source namespace name. */
    @Lazy
    volatile Map<String, NamespaceContents> discovered = discover()

    /** Source namespace name -> target naming decision. */
    @Lazy
    volatile Map<String, NamespaceMapper.Mapping> namespaceMappings = resolveNames()

    @Lazy
    volatile ReplicationGroupResolver groupResolver =
            new ReplicationGroupResolver(targetClient.listReplicationGroups(), migrationConfig.replicationGroup)

    /**
     * Discovery happens the first time the task list is built, which is during {@code run()}.
     */
    @Lazy
    volatile List<? extends AbstractMigrationTask> taskList = buildTaskList()

    private Map<String, NamespaceContents> discover() {
        // replication group IDs are cluster-specific, so record the name for matching on the target
        def sourceGroupNames = [:]
        sourceClient.listReplicationGroups().each { sourceGroupNames[it.id] = it.name }

        def result = [:]
        migrationConfig.selectedNamespaces.each { namespace ->
            def contents = new NamespaceContents()
            contents.users = sourceClient.listObjectUsers(namespace)
            contents.buckets = sourceClient.listBuckets(namespace)
            contents.buckets.each { it.replicationGroupName = sourceGroupNames[it.replicationGroupId] }

            result[namespace] = contents
            log.info("discovered namespace ${namespace}: ${contents.users.size()} users, ${contents.buckets.size()} buckets")
        }
        result
    }

    private Map<String, NamespaceMapper.Mapping> resolveNames() {
        def existing = targetClient.listNamespaces().collect { it.name } as Set
        def mapper = new NamespaceMapper(migrationConfig.targetNamespaceSuffix, [:], existing)
        def mappings = [:]
        migrationConfig.selectedNamespaces.each { mappings[it] = mapper.map(it) }
        mappings
    }

    private List<? extends AbstractMigrationTask> buildTaskList() {
        migrationConfig.selectedNamespaces.collect { namespace ->
            def mapping = namespaceMappings[namespace]
            def contents = discovered[namespace]

            def tasks = []
            tasks << new CreateNamespaceTask([migration: this, sourceNamespace: namespace])
            contents.users.each { EcsObjectUser user ->
                tasks << new CreateUserTask([migration: this, sourceNamespace: namespace, sourceUserId: user.userId])
            }
            contents.buckets.each { EcsBucketInfo bucket ->
                tasks << new CreateBucketTask([migration: this, sourceNamespace: namespace, bucket: bucket])
                tasks << new BucketSyncTask([migration: this, sourceNamespace: namespace, bucket: bucket,
                                             taskWeight: 1000])
            }
            tasks
        }.flatten() as List<AbstractMigrationTask>
    }

    /**
     * Chooses the identity used to read a bucket on the source and write it on the target. The bucket owner is
     * preferred so ownership is preserved; otherwise any user in the namespace is used.
     *
     * @return the source-side user id, or null if the namespace has no object users
     */
    String resolveBucketUser(String sourceNamespace, EcsBucketInfo bucket) {
        def users = discovered[sourceNamespace].users
        if (bucket.owner && users.find { it.userId == bucket.owner }) return bucket.owner
        users ? users.first().userId : null
    }

    String targetNamespaceFor(String sourceNamespace) {
        namespaceMappings[sourceNamespace].targetNamespace
    }

    String targetUserFor(String sourceNamespace, String sourceUserId) {
        namespaceMappings[sourceNamespace].mapUserId(sourceUserId)
    }

    /** Only call after all services have been injected */
    def testConfig() {
        try {
            return runTestConfig()
        } finally {
            // the migration object is discarded after a test, so do not leak the clients
            closeClients()
        }
    }

    private runTestConfig() {
        def testResult = [
                source        : [status: 'danger', message: ''],
                target        : [status: 'danger', message: ''],
                replicationGrp: [status: 'danger', message: ''],
                sameCluster   : [status: 'success', message: ''],
                namespaces    : [:]
        ]

        // source management access
        List<EcsNamespace> sourceNamespaces = []
        try {
            sourceNamespaces = sourceClient.listNamespaces()
            testResult.source.status = 'success'
            testResult.source.message = "Connected; ${sourceNamespaces.size()} namespaces visible"
        } catch (e) {
            testResult.source.message = e.message
            return testResult
        }

        // target management access
        List<String> targetNamespaceNames = []
        try {
            targetNamespaceNames = targetClient.listNamespaces().collect { it.name }
            testResult.target.status = 'success'
            testResult.target.message = "Connected; ${targetNamespaceNames.size()} namespaces visible"
        } catch (e) {
            testResult.target.message = e.message
            return testResult
        }

        // replication group must exist on the target
        try {
            new ReplicationGroupResolver(targetClient.listReplicationGroups(), migrationConfig.replicationGroup)
            testResult.replicationGrp.status = 'success'
            testResult.replicationGrp.message = "Replication group '${migrationConfig.replicationGroup}' exists"
        } catch (e) {
            testResult.replicationGrp.message = e.message
        }

        // same-cluster runs are a storage pool migration; every namespace will be renamed
        if (ClusterEndpoints.sameCluster(sourceStorage as EcsStorage, targetStorage as EcsStorage)) {
            testResult.sameCluster.status = 'warning'
            testResult.sameCluster.message = 'Source and target are the same cluster (storage pool migration). ' +
                    'Every namespace and object user will be recreated under a new name.'
        } else {
            testResult.sameCluster.message = 'Source and target are different clusters'
        }

        // per-namespace: does it exist on the source, and what will it be called on the target?
        def mapper = new NamespaceMapper(migrationConfig.targetNamespaceSuffix, [:], targetNamespaceNames as Set)
        migrationConfig.selectedNamespaces.each { namespace ->
            def entry = [:]
            if (!sourceNamespaces.find { it.name == namespace }) {
                entry.status = 'danger'
                entry.message = "Namespace does not exist on the source cluster"
            } else {
                def mapping = mapper.map(namespace)
                try {
                    def users = sourceClient.listObjectUsers(namespace)
                    def buckets = sourceClient.listBuckets(namespace)
                    if (!users) {
                        entry.status = 'danger'
                        entry.message = "Namespace has no object users, so its data cannot be read or written"
                    } else if (mapping.renamed) {
                        entry.status = 'warning'
                        entry.message = "Will be created as '${mapping.targetNamespace}' " +
                                "(users suffixed with '${mapping.appliedSuffix}'): " +
                                "${users.size()} users, ${buckets.size()} buckets"
                    } else {
                        entry.status = 'success'
                        entry.message = "Will be created as '${mapping.targetNamespace}': " +
                                "${users.size()} users, ${buckets.size()} buckets"
                    }
                } catch (e) {
                    entry.status = 'danger'
                    entry.message = e.message
                }
            }
            testResult.namespaces[namespace] = entry
        }

        testResult
    }

    static class NamespaceContents {
        List<EcsObjectUser> users = []
        List<EcsBucketInfo> buckets = []
    }
}

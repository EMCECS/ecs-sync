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
import com.emc.object.s3.S3Exception
import com.emc.object.s3.S3ObjectMetadata
import com.emc.object.s3.bean.CannedAcl
import com.emc.object.s3.request.PutObjectRequest
import grails.util.GrailsUtil
import groovy.transform.EqualsAndHashCode
import sync.ui.EcsBucket
import sync.ui.config.EcsConfigService
import sync.ui.migration.AbstractMigration
import sync.ui.migration.AbstractMigrationTask
import sync.ui.migration.WaitTask
import sync.ui.storage.AtmosStorage
import sync.ui.storage.EcsStorage
import sync.ui.storage.StorageEntry

class A2EMigration extends AbstractMigration<A2EMigrationConfig> {
    def atmosService
    def ecsService

    @Lazy
    volatile atmosSubtenant = atmosService.getSubtenantDetails(sourceStorage as AtmosStorage, migrationConfig.atmosSubtenantName).subtenant

    def ecsBucket = new S3ProxyBucket()

    // TODO: accessing the bucket should probably be moved to a service
    @Lazy
    volatile s3ProxyClient = EcsConfigService.getEcsClient(ecsBucket)

    @Lazy
    volatile List<? extends AbstractMigrationTask> taskList = [
            // create each unique namespace in the mapping config
            migrationConfig.userNamespaceMap.values().unique(false).collect { namespace ->
                new CreateNamespaceTask([namespaceName: namespace, migration: this])
            },
            migrationConfig.userMap.collect { k, v ->
                new CreateUserTask([atmosUser: k, ecsUser: v, migration: this])
            },
            new S3ProxySubtenantPreMigrationTask([migration: this]),
            new WaitTask([waitMilliseconds: migrationConfig.s3ProxyPollIntervalSecs * 1000, reason: 'S3 Proxy update interval']),
            new S3ProxyDiscoverBucketsTask([migration: this]),
            new S3ProxyUserMapTask([migration: this]),
            new S3ProxySubtenantMigratingTask([migration: this]),
            new WaitTask([waitMilliseconds: migrationConfig.s3ProxyPollIntervalSecs * 1000, reason: 'S3 Proxy update interval']),
            migrationConfig.selectedUsers.collect { user ->
                    atmosService.listBucketsForUser(sourceStorage as AtmosStorage, migrationConfig.atmosSubtenantName, user).collect { bucket ->
                    def syncTask = new S3SyncTask([migration: this, atmosUser: user, bucket: bucket, taskWeight: 1000])
                    if (migrationConfig.useS3Proxy)
                        [
                                new CreateBucketTask(migration: this, atmosUser: user, bucket: bucket),
                                new BucketPreMigrationTask(migration: this, sourceUser: user, bucket: bucket),
                                new S3ProxyBucketMigratingTask([migration: this, bucket: bucket]),
                                new WaitTask([waitMilliseconds: migrationConfig.s3ProxyPollIntervalSecs * 1000, reason: 'S3 Proxy update interval']),
                                new WaitTask([waitMilliseconds: migrationConfig.s3ProxyPreBucketMigrationWaitSecs * 1000, reason: 'S3 Proxy pre-migration bucket wait']),
                                syncTask,
                                new S3ProxyBucketPostMigrationTask([migration: this, bucket: bucket])
                        ]
                    else
                        [
                                new CreateBucketTask(migration: this, atmosUser: user, bucket: bucket),
                                new BucketPreMigrationTask(migration: this, sourceUser: user, bucket: bucket),
                                syncTask
                        ]
                }
            },
            new S3ProxySubtenantPostMigrationTask([migration: this])
    ].flatten() as List<AbstractMigrationTask>

    def s3ProxyObjectExists(key) {
        try {
            s3ProxyClient.getObjectMetadata(ecsBucket.bucket, key)
            return true
        } catch (S3Exception e) {
            if (e.httpCode != 404) throw e
        }
    }

    def s3ProxyCreateObject(key, content = null) {
        s3ProxyClient.putObject(new PutObjectRequest(ecsBucket.bucket, key, content)
                .withObjectMetadata(new S3ObjectMetadata([contentType: 'text/plain']))
                .withCannedAcl(CannedAcl.PublicRead))
    }

    def sourceUserS3Client(username) {
        def sourceEndpoint = (sourceStorage as AtmosStorage).s3ApiEndpoint.toURI()
        String sourceUser = "${atmosSubtenant.id}/${username}"
        String sourceKey = atmosSubtenant.objectUsers.find { it.uid == username }.sharedSecret
        return EcsConfigService.getEcsClient(new EcsBucket() {
            String hosts = sourceEndpoint.getHost()
            String protocol = sourceEndpoint.getScheme()
            int port = sourceEndpoint.getPort()
            boolean smartClient = false
            String accessKey = sourceUser
            String secretKey = sourceKey
            String bucket = ''
        })
    }

    def targetUserS3Client(sourceUser) {
        def ecsStorage = targetStorage as EcsStorage
        def targetUser = migrationConfig.userMap[sourceUser]
        def namespace = migrationConfig.userNamespaceMap[sourceUser]
        String targetKey = ecsService.getSecretKey(ecsStorage, targetUser, namespace).secretKey1
        def targetEndpoint = (targetStorage as EcsStorage).s3ApiEndpoint.toURI()
        return EcsConfigService.getEcsClient(new EcsBucket() {
            String hosts = targetEndpoint.getHost()
            String protocol = targetEndpoint.getScheme()
            int port = targetEndpoint.getPort()
            boolean smartClient = false
            String accessKey = targetUser
            String secretKey = targetKey
            String bucket = ''
        })
    }

    /** Only call after all services have been injected */
    def testConfig() {
        def testResult = [
                atmosSubtenant: [
                        status : 'danger',
                        message: ''
                ],
                ecsNamespace  : [
                        status : 'danger',
                        message: ''
                ],
                userMapping   : [
                        status : 'success',
                        message: ''
                ],
                users         : [:],
                s3Proxy       : [
                        status : 'danger',
                        message: ''
                ]
        ]

        // check subtenant in Atmos
        def subtenantId
        try {
            subtenantId = atmosSubtenant.id
            testResult.atmosSubtenant.status = 'success'
            testResult.atmosSubtenant.message = 'Subtenant exists'
        } catch (e) {
            testResult.atmosSubtenant.message = e.message
        }

        def ecs = new StorageEntry([xmlKey: migrationConfig.targetXmlKey, configService: configService]).storage as EcsStorage

        // check namespaces in ECS
        def namespaceProblems = [:]
        migrationConfig.userNamespaceMap.values().unique(false).each { ecsNamespace ->
            try {
                ecsService.getNamespaceInfo(ecs, ecsNamespace)
                // namespace exists, this is fine
            } catch (e) {
                e = GrailsUtil.extractRootCause(e)
                if (e instanceof EcsException && e.errorCode == '1004') {
                    // namespace will be created, this is fine too
                } else {
                    namespaceProblems[ecsNamespace] = e.message
                }
            }
        }

        if (migrationConfig.useS3Proxy) {
            def s3Client = EcsConfigService.getEcsClient(ecsBucket)
            try {

                // check S3 Proxy bucket connection
                EcsConfigService.getEcsClient(ecsBucket)
                if (s3Client.bucketExists(migrationConfig.s3ProxyBucket)) {
                    testResult.s3Proxy.status = 'success'
                    testResult.s3Proxy.message = 'S3 Proxy bucket exists'
                } else {
                    testResult.s3Proxy.message = 'S3 Proxy bucket does not exist'
                }

                // check if user mapping file exists
                if (subtenantId && s3ProxyObjectExists(AbstractS3ProxyTask.bucketsPreMigKey(subtenantId))) {
                    testResult.userMapping.status = 'warning'
                    testResult.userMapping.message = 'User mapping already exists and cannot be modified'
                } else {
                    testResult.userMapping.message = 'User mapping will be created (please ensure all users are mapped!)'
                }
            } catch (e) {
                testResult.s3Proxy.message = e.message
            } finally {
                // shut down s3 client (stops polling threads)
                s3Client.destroy()
            }
        }

        // check if users exist in the Atmos subtenant and in ECS *within the namespace*
        migrationConfig.userMap.each { sUser, tUser ->
            testResult.users[sUser] = [:]

            // mapped namespace for this user
            def ecsNamespace = migrationConfig.userNamespaceMap[sUser]

            // problem with namespace?
            if (namespaceProblems[ecsNamespace]) {
                testResult.users[sUser].status = 'danger'
                testResult.users[sUser].message = "Namespace problem: ${namespaceProblems[ecsNamespace]}"
            } else {

                // user exists in Atmos? (in case of uploaded user mapping)
                if (!atmosSubtenant.objectUsers.find { it.uid == sUser }) {
                    testResult.users[sUser].status = 'danger'
                    testResult.users[sUser].message = "User ${sUser} does not exist in Atmos subtenant"
                } else {
                    try {
                        // user exists in ECS?
                        def ecsUser = ecsService.getObjectUserInfo(ecs, tUser, ecsNamespace)
                        if (ecsUser.namespace.toString() == ecsNamespace) {
                            testResult.users[sUser].status = 'warning'
                            testResult.users[sUser].message = 'User already exists in ECS; if a source bucket already exists in the target, data may be overwritten'
                        } else {
                            // user is in a different namespace
                            testResult.users[sUser].status = 'danger'
                            testResult.users[sUser].message = "User ${tUser} exists in a different namespace (${ecsUser.namespace.toString()})"
                        }
                    } catch (e) {
                        e = GrailsUtil.extractRootCause(e)
                        if (e instanceof EcsException && e.httpCode == 404) {
                            testResult.users[sUser].status = 'success'
                            testResult.users[sUser].message = "User will be created"
                        } else {
                            testResult.users[sUser].status = 'danger'
                            testResult.users[sUser].message = e.message
                        }
                    }
                }
            }
        }

        testResult
    }

    @EqualsAndHashCode
    private class S3ProxyBucket implements EcsBucket {
        @Override
        String getHosts() {
            return migrationConfig.s3ProxyBucketEndpoint.toURI().getHost()
        }

        @Override
        String getProtocol() {
            return migrationConfig.s3ProxyBucketEndpoint.toURI().getScheme()
        }

        @Override
        int getPort() {
            return migrationConfig.s3ProxyBucketEndpoint.toURI().getPort()
        }

        @Override
        boolean isSmartClient() {
            return migrationConfig.s3ProxyBucketSmartClient
        }

        @Override
        String getAccessKey() {
            return migrationConfig.s3ProxyBucketAccessKey
        }

        @Override
        String getSecretKey() {
            return migrationConfig.s3ProxyBucketSecretKey
        }

        @Override
        String getBucket() {
            return migrationConfig.s3ProxyBucket
        }
    }
}

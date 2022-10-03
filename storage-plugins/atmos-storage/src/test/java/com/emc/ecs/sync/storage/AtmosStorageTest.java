/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage;

import com.emc.atmos.api.*;
import com.emc.atmos.api.bean.ListObjectsResponse;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.api.bean.ObjectEntry;
import com.emc.atmos.api.bean.ReadObjectResponse;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.api.request.CreateObjectRequest;
import com.emc.atmos.api.request.ListObjectsRequest;
import com.emc.atmos.api.request.ReadObjectRequest;
import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.test.TestUtil;
import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AtmosStorageTest {
    private static final String SEARCH_KEY = "storage-test-search-key";

    private Protocol protocol, protocol2;
    private List<String> hosts, hosts2;
    private int port, port2;
    private String uid, uid2;
    private String secretKey, secretKey2;
    private AtmosApi atmos1, atmos2;
    private boolean isEcs;

    @BeforeEach
    public void setup() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();

        String endpoints = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_ENDPOINTS);
        uid = syncProperties.getProperty(TestConfig.PROP_ATMOS_UID);
        secretKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_SECRET);
        Assumptions.assumeTrue(endpoints != null && uid != null && secretKey != null);
        isEcs = Boolean.parseBoolean(syncProperties.getProperty(TestConfig.PROP_ATMOS_IS_ECS));

        String endpoints2 = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_ENDPOINTS + 2);
        uid2 = syncProperties.getProperty(TestConfig.PROP_ATMOS_UID + 2);
        secretKey2 = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_SECRET + 2);
        String isEcsStr2 = syncProperties.getProperty(TestConfig.PROP_ATMOS_IS_ECS + 2);
        Assumptions.assumeTrue(endpoints2 != null && uid2 != null && secretKey2 != null && isEcsStr2 != null);
        Assumptions.assumeTrue(Boolean.parseBoolean(isEcsStr2));

        protocol = Protocol.http;
        hosts = new ArrayList<>();
        List<URI> uris = new ArrayList<>();
        port = -1;
        for (String endpoint : endpoints.split(",")) {
            URI uri = new URI(endpoint);
            uris.add(uri);
            protocol = Protocol.valueOf(uri.getScheme().toLowerCase());
            port = uri.getPort();
            hosts.add(uri.getHost());
        }

        protocol2 = Protocol.http;
        hosts2 = new ArrayList<>();
        List<URI> uris2 = new ArrayList<>();
        port2 = -1;
        for (String endpoint : endpoints2.split(",")) {
            URI uri = new URI(endpoint);
            uris2.add(uri);
            protocol2 = Protocol.valueOf(uri.getScheme().toLowerCase());
            port2 = uri.getPort();
            hosts2.add(uri.getHost());
        }

        atmos1 = new AtmosApiClient(new AtmosConfig(uid, secretKey, uris.toArray(new URI[uris.size()])));
        atmos2 = new AtmosApiClient(new AtmosConfig(uid2, secretKey2, uris2.toArray(new URI[uris2.size()])));
    }

    @AfterEach
    public void teardown() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(32);
        List<Future> futures = new ArrayList<>();
        if (atmos1 != null) {
            ListObjectsResponse response = atmos1.listObjects(new ListObjectsRequest().metadataName(SEARCH_KEY));
            if (response.getEntries() != null) {
                for (final ObjectEntry entry : response.getEntries()) {
                    futures.add(service.submit(new Runnable() {
                        @Override
                        public void run() {
                            atmos1.delete(entry.getObjectId());
                        }
                    }));
                }
            }
        }
        if (atmos2 != null) {
            ListObjectsResponse response = atmos2.listObjects(new ListObjectsRequest().metadataName(SEARCH_KEY));
            if (response.getEntries() != null) {
                for (final ObjectEntry entry : response.getEntries()) {
                    futures.add(service.submit(new Runnable() {
                        @Override
                        public void run() {
                            atmos2.delete(entry.getObjectId());
                        }
                    }));
                }
            }
        }
        for (Future future : futures) {
            future.get();
        }
    }

    @Test
    public void testPreserveOid() throws Exception {
        Assumptions.assumeFalse(isEcs);

        byte[] data = new byte[1111];
        new Random().nextBytes(data);

        // create 10 objects
        StringBuilder oids = new StringBuilder();
        List<ObjectId> oidList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ObjectId oid = atmos1.createObject(new CreateObjectRequest().content(data).userMetadata(new Metadata(SEARCH_KEY, "foo", true))).getObjectId();
            oids.append(oid.getId()).append("\n");
            oidList.add(oid);
        }

        File oidFile = TestUtil.writeTempFile(oids.toString());

        // disable retries
        SyncOptions options = new SyncOptions().withSourceListFile(oidFile.getAbsolutePath()).withRetryAttempts(0);

        com.emc.ecs.sync.config.storage.AtmosConfig atmosConfig1 = new com.emc.ecs.sync.config.storage.AtmosConfig();
        atmosConfig1.setProtocol(protocol);
        atmosConfig1.setHosts(hosts.toArray(new String[hosts.size()]));
        atmosConfig1.setPort(port);
        atmosConfig1.setUid(uid);
        atmosConfig1.setSecret(secretKey);
        atmosConfig1.setAccessType(com.emc.ecs.sync.config.storage.AtmosConfig.AccessType.objectspace);

        com.emc.ecs.sync.config.storage.AtmosConfig atmosConfig2 = new com.emc.ecs.sync.config.storage.AtmosConfig();
        atmosConfig2.setProtocol(protocol2);
        atmosConfig2.setHosts(hosts2.toArray(new String[hosts2.size()]));
        atmosConfig2.setPort(port2);
        atmosConfig2.setUid(uid2);
        atmosConfig2.setSecret(secretKey2);
        atmosConfig2.setAccessType(com.emc.ecs.sync.config.storage.AtmosConfig.AccessType.objectspace);
        atmosConfig2.setPreserveObjectId(true);

        SyncConfig syncConfig = new SyncConfig().withOptions(options).withSource(atmosConfig1).withTarget(atmosConfig2);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        TestUtil.run(sync);

        Assertions.assertEquals(10, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

        // manually verify objects in target
        for (ObjectId oid : oidList) {
            Assertions.assertArrayEquals(data, atmos2.readObject(oid, byte[].class));
        }
    }

    @Test
    public void testWsChecksumOids() throws Exception {
        Assumptions.assumeFalse(isEcs);

        byte[] data = new byte[1211];
        Random random = new Random();

        // create 10 objects with SHA0 wschecksum
        StringBuilder oids = new StringBuilder();
        List<ObjectId> oidList = new ArrayList<>();
        RunningChecksum checksum;
        for (int i = 0; i < 10; i++) {
            random.nextBytes(data);
            checksum = new RunningChecksum(ChecksumAlgorithm.SHA0);
            checksum.update(data, 0, data.length);
            ObjectId oid = atmos1.createObject(new CreateObjectRequest().content(data).wsChecksum(checksum)
                    .userMetadata(new Metadata(SEARCH_KEY, "foo", true))).getObjectId();
            oids.append(oid.getId()).append("\n");
            oidList.add(oid);
        }

        File oidFile = TestUtil.writeTempFile(oids.toString());

        // disable retries
        SyncOptions options = new SyncOptions().withSourceListFile(oidFile.getAbsolutePath()).withRetryAttempts(0);

        com.emc.ecs.sync.config.storage.AtmosConfig atmosConfig1 = new com.emc.ecs.sync.config.storage.AtmosConfig();
        atmosConfig1.setProtocol(protocol);
        atmosConfig1.setHosts(hosts.toArray(new String[hosts.size()]));
        atmosConfig1.setPort(port);
        atmosConfig1.setUid(uid);
        atmosConfig1.setSecret(secretKey);
        atmosConfig1.setAccessType(com.emc.ecs.sync.config.storage.AtmosConfig.AccessType.objectspace);

        com.emc.ecs.sync.config.storage.AtmosConfig atmosConfig2 = new com.emc.ecs.sync.config.storage.AtmosConfig();
        atmosConfig2.setProtocol(protocol2);
        atmosConfig2.setHosts(hosts2.toArray(new String[hosts2.size()]));
        atmosConfig2.setPort(port2);
        atmosConfig2.setUid(uid2);
        atmosConfig2.setSecret(secretKey2);
        atmosConfig2.setAccessType(com.emc.ecs.sync.config.storage.AtmosConfig.AccessType.objectspace);
        atmosConfig2.setPreserveObjectId(true);

        SyncConfig syncConfig = new SyncConfig().withOptions(options).withSource(atmosConfig1).withTarget(atmosConfig2);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        TestUtil.run(sync);

        Assertions.assertEquals(10, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

        // manually verify objects in target
        for (ObjectId oid : oidList) {
            verifyChecksummedObject(oid);
        }
    }


    @Test
    public void testWsChecksumNamespace() throws Exception {
        Assumptions.assumeFalse(isEcs);

        byte[] data = new byte[1511];
        Random random = new Random();

        // create 10 objects with SHA0 wschecksum
        StringBuilder paths = new StringBuilder();
        List<ObjectPath> pathList = new ArrayList<>();
        RunningChecksum checksum;
        for (int i = 0; i < 10; i++) {
            random.nextBytes(data);
            checksum = new RunningChecksum(ChecksumAlgorithm.SHA0);
            checksum.update(data, 0, data.length);
            ObjectPath path = new ObjectPath("/wsChecksumTest/object-" + i);
            atmos1.createObject(new CreateObjectRequest().identifier(path).content(data).wsChecksum(checksum)
                    .userMetadata(new Metadata(SEARCH_KEY, "foo", true))).getObjectId();
            paths.append(path.getPath()).append("\n");
            pathList.add(path);
        }

        File pathFile = TestUtil.writeTempFile(paths.toString());

        // disable retries
        SyncOptions options = new SyncOptions().withSourceListFile(pathFile.getAbsolutePath()).withRetryAttempts(0);

        com.emc.ecs.sync.config.storage.AtmosConfig atmosConfig1 = new com.emc.ecs.sync.config.storage.AtmosConfig();
        atmosConfig1.setProtocol(protocol);
        atmosConfig1.setHosts(hosts.toArray(new String[hosts.size()]));
        atmosConfig1.setPort(port);
        atmosConfig1.setUid(uid);
        atmosConfig1.setSecret(secretKey);
        atmosConfig1.setAccessType(com.emc.ecs.sync.config.storage.AtmosConfig.AccessType.namespace);

        com.emc.ecs.sync.config.storage.AtmosConfig atmosConfig2 = new com.emc.ecs.sync.config.storage.AtmosConfig();
        atmosConfig2.setProtocol(protocol2);
        atmosConfig2.setHosts(hosts2.toArray(new String[hosts2.size()]));
        atmosConfig2.setPort(port2);
        atmosConfig2.setUid(uid2);
        atmosConfig2.setSecret(secretKey2);
        atmosConfig2.setAccessType(com.emc.ecs.sync.config.storage.AtmosConfig.AccessType.namespace);

        SyncConfig syncConfig = new SyncConfig().withOptions(options).withSource(atmosConfig1).withTarget(atmosConfig2);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        TestUtil.run(sync);

        Assertions.assertEquals(10, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

        // manually verify objects in target
        for (ObjectPath path : pathList) {
            verifyChecksummedObject(path);
        }
    }

    // NOTE: this seems to fail on ECS 3.6 because grants on directories don't seem to apply as they used to.
    //       the same issue also causes EndToEndTest.testAtmos to fail when ACL verification is on
    @Test
    public void testAcl() {
        com.emc.ecs.sync.config.storage.AtmosConfig atmosConfig = new com.emc.ecs.sync.config.storage.AtmosConfig();
        atmosConfig.setProtocol(protocol);
        atmosConfig.setHosts(hosts.toArray(new String[hosts.size()]));
        atmosConfig.setPort(port);
        atmosConfig.setUid(uid);
        atmosConfig.setSecret(secretKey);
        atmosConfig.setAccessType(com.emc.ecs.sync.config.storage.AtmosConfig.AccessType.namespace);

        AtmosStorage atmosStorage = new AtmosStorage();
        atmosStorage.withConfig(atmosConfig).withOptions(new SyncOptions());
        atmosStorage.configure(null, Collections.emptyIterator(), atmosStorage);

        String owner = uid.substring(uid.lastIndexOf('/') + 1);
        ObjectAcl acl = new ObjectAcl();
        acl.setOwner(owner);
        acl.addUserGrant(owner, "FULL_CONTROL");
        acl.addGroupGrant("other", "NONE");

        ObjectMetadata metadata = new ObjectMetadata().withContentLength(0).withContentType("application/octet-stream");
        metadata.setUserMetadataValue(SEARCH_KEY, "foo", true);
        SyncObject object = new SyncObject(new TestStorage(), "foo", metadata,
                new ByteArrayInputStream(new byte[0]), acl);

        metadata = new ObjectMetadata().withContentLength(0).withContentType("application/x-directory").withDirectory(true);
        metadata.setUserMetadataValue(SEARCH_KEY, "foo", true);
        SyncObject dir = new SyncObject(new TestStorage(), "bar", metadata,
                new ByteArrayInputStream(new byte[0]), acl);

        String objectId = atmosStorage.createObject(object);
        String dirId = atmosStorage.createObject(dir);

        Assertions.assertEquals(object.getAcl(), atmosStorage.loadObject(objectId).getAcl());
        Assertions.assertEquals(object.getAcl(), atmosStorage.loadObject(dirId).getAcl());
    }

    private void verifyChecksummedObject(ObjectIdentifier id) throws IOException {
        byte[] data = atmos1.readObject(id, byte[].class);
        ReadObjectResponse<byte[]> response = atmos2.readObject(new ReadObjectRequest().identifier(id), byte[].class);
        Assertions.assertArrayEquals(data, response.getObject());
        Assertions.assertNotNull(response.getWsChecksum());
        Assertions.assertEquals(ChecksumAlgorithm.SHA0, response.getWsChecksum().getAlgorithm());
    }
}

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
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.test.TestUtil;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
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

    @Before
    public void setup() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();

        String endpoints = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_ENDPOINTS);
        uid = syncProperties.getProperty(TestConfig.PROP_ATMOS_UID);
        secretKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_SECRET);
        String isEcsStr = syncProperties.getProperty(TestConfig.PROP_ATMOS_IS_ECS);
        Assume.assumeNotNull(endpoints, uid, secretKey);
        Assume.assumeFalse(isEcsStr != null && Boolean.parseBoolean(isEcsStr));

        String endpoints2 = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_ENDPOINTS + 2);
        uid2 = syncProperties.getProperty(TestConfig.PROP_ATMOS_UID + 2);
        secretKey2 = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_SECRET + 2);
        String isEcsStr2 = syncProperties.getProperty(TestConfig.PROP_ATMOS_IS_ECS + 2);
        Assume.assumeNotNull(endpoints2, uid2, secretKey2, isEcsStr2);
        Assume.assumeTrue(Boolean.parseBoolean(isEcsStr2));

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

    @After
    public void teardown() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(32);
        List<Future> futures = new ArrayList<>();
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
        response = atmos2.listObjects(new ListObjectsRequest().metadataName(SEARCH_KEY));
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
        for (Future future : futures) {
            future.get();
        }
    }

    @Test
    public void testPreserveOid() throws Exception {
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
        sync.run();

        Assert.assertEquals(10, sync.getStats().getObjectsComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        // manually verify objects in target
        for (ObjectId oid : oidList) {
            Assert.assertArrayEquals(data, atmos2.readObject(oid, byte[].class));
        }
    }

    @Test
    public void testWsChecksumOids() throws Exception {
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
        sync.run();

        Assert.assertEquals(10, sync.getStats().getObjectsComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        // manually verify objects in target
        for (ObjectId oid : oidList) {
            verifyChecksummedObject(oid);
        }
    }


    @Test
    public void testWsChecksumNamespace() throws Exception {
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
        sync.run();

        Assert.assertEquals(10, sync.getStats().getObjectsComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        // manually verify objects in target
        for (ObjectPath path : pathList) {
            verifyChecksummedObject(path);
        }
    }

    private void verifyChecksummedObject(ObjectIdentifier id) throws IOException {
        byte[] data = atmos1.readObject(id, byte[].class);
        ReadObjectResponse<byte[]> response = atmos2.readObject(new ReadObjectRequest().identifier(id), byte[].class);
        Assert.assertArrayEquals(data, response.getObject());
        Assert.assertNotNull(response.getWsChecksum());
        Assert.assertEquals(ChecksumAlgorithm.SHA0, response.getWsChecksum().getAlgorithm());
    }
}

/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.ecs.sync;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.emc.ecs.sync.config.LogLevel;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.*;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.service.AbstractDbService;
import com.emc.ecs.sync.service.SqliteDbService;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.util.PluginUtil;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.jersey.S3JerseyClient;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.File;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

public class EndToEndTest {
    private static final Logger log = LoggerFactory.getLogger(EndToEndTest.class);

    private static final int SM_OBJ_COUNT = 200;
    private static final int SM_OBJ_MAX_SIZE = 10240; // 10K
    private static final int LG_OBJ_COUNT = 10;
    private static final int LG_OBJ_MAX_SIZE = 1024 * 1024; // 1M

    private static final int SYNC_THREAD_COUNT = 32;

    private ExecutorService service;

    private class TestDbService extends SqliteDbService {
        TestDbService() {
            super(":memory:");
            initCheck();
        }

        @Override
        public JdbcTemplate getJdbcTemplate() {
            return super.getJdbcTemplate();
        }
    }

    private TestDbService dbService = new TestDbService();

    @Before
    public void before() {
        service = Executors.newFixedThreadPool(SYNC_THREAD_COUNT);
    }

    @After
    public void after() {
        if (service != null) service.shutdownNow();
    }

    @Test
    public void testTestPlugins() throws Exception {
        TestConfig config = new TestConfig().withObjectCount(SM_OBJ_COUNT).withMaxSize(SM_OBJ_MAX_SIZE)
                .withReadData(true).withDiscardData(false);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withSource(config).withTarget(config));
        sync.run();

        TestStorage source = (TestStorage) sync.getSource();
        TestStorage target = (TestStorage) sync.getTarget();

        VerifyTest.verifyObjects(source, source.getRootObjects(), target, target.getRootObjects(), true);
    }

    @Test
    public void testFilesystem() throws Exception {
        final File tempDir = new File("/tmp/ecs-sync-filesystem-test"); // File.createTempFile("ecs-sync-filesystem-test", "dir");
        tempDir.mkdir();
        tempDir.deleteOnExit();

        if (!tempDir.exists() || !tempDir.isDirectory())
            throw new RuntimeException("unable to make temp dir");

        FilesystemConfig filesystemConfig = new FilesystemConfig();
        filesystemConfig.setPath(tempDir.getPath());
        filesystemConfig.setStoreMetadata(true);

        multiEndToEndTest(filesystemConfig, new TestConfig(), false);

        new File(tempDir, ObjectMetadata.METADATA_DIR).delete(); // delete this so the temp dir can go away
    }

    @Test
    public void testArchive() throws Exception {
        final File archive = new File("/tmp/ecs-sync-archive-test.zip");
        if (archive.exists()) archive.delete();
        archive.deleteOnExit();

        ArchiveConfig archiveConfig = new ArchiveConfig();
        archiveConfig.setPath(archive.getPath());
        archiveConfig.setStoreMetadata(true);

        TestConfig testConfig = new TestConfig().withReadData(true).withDiscardData(false);
        testConfig.withObjectCount(LG_OBJ_COUNT).withMaxSize(LG_OBJ_MAX_SIZE);

        endToEndTest(archiveConfig, testConfig, null, false);
    }

    @Test
    public void testAtmos() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();
        final String rootPath = "/ecs-sync-atmos-test/";
        String endpoints = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_ENDPOINTS);
        String uid = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_UID);
        String secretKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_SECRET);
        Assume.assumeNotNull(endpoints, uid, secretKey);

        Protocol protocol = Protocol.http;
        List<String> hosts = new ArrayList<>();
        int port = -1;
        for (String endpoint : endpoints.split(",")) {
            URI uri = new URI(endpoint);
            protocol = Protocol.valueOf(uri.getScheme().toLowerCase());
            port = uri.getPort();
            hosts.add(uri.getHost());
        }

        AtmosConfig atmosConfig = new AtmosConfig();
        atmosConfig.setProtocol(protocol);
        atmosConfig.setHosts(hosts.toArray(new String[hosts.size()]));
        atmosConfig.setPort(port);
        atmosConfig.setUid(uid);
        atmosConfig.setSecret(secretKey);
        atmosConfig.setPath(rootPath);
        atmosConfig.setAccessType(AtmosConfig.AccessType.namespace);

        String[] validGroups = new String[]{"other"};
        String[] validPermissions = new String[]{"READ", "WRITE", "FULL_CONTROL"};

        TestConfig testConfig = new TestConfig();
        testConfig.setObjectOwner(uid.substring(uid.lastIndexOf('/') + 1));
        testConfig.setValidGroups(validGroups);
        testConfig.setValidPermissions(validPermissions);

        ObjectAcl template = new ObjectAcl();
        template.addGroupGrant("other", "NONE");

        multiEndToEndTest(atmosConfig, testConfig, template, true);
    }

    @Test
    public void testEcsS3() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();
        final String bucket = "ecs-sync-s3-test-bucket";
        final String endpoint = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_SECRET_KEY);
        final boolean useVHost = Boolean.valueOf(syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_VHOST));
        Assume.assumeNotNull(endpoint, accessKey, secretKey);
        URI endpointUri = new URI(endpoint);

        S3Config s3Config;
        if (useVHost) {
            s3Config = new S3Config(endpointUri);
        } else {
            s3Config = new S3Config(com.emc.object.Protocol.valueOf(endpointUri.getScheme().toUpperCase()), endpointUri.getHost());
        }
        s3Config.withPort(endpointUri.getPort()).withUseVHost(useVHost).withIdentity(accessKey).withSecretKey(secretKey);

        S3Client s3 = new S3JerseyClient(s3Config);

        try {
            s3.createBucket(bucket);
        } catch (S3Exception e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        // for testing ACLs
        String authUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
        String everyone = "http://acs.amazonaws.com/groups/global/AllUsers";
        String[] validGroups = {authUsers, everyone};
        String[] validPermissions = {"READ", "WRITE", "FULL_CONTROL"};

        EcsS3Config ecsS3Config = new EcsS3Config();
        if (endpointUri.getScheme() != null)
            ecsS3Config.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
        ecsS3Config.setHost(endpointUri.getHost());
        ecsS3Config.setPort(endpointUri.getPort());
        ecsS3Config.setAccessKey(accessKey);
        ecsS3Config.setSecretKey(secretKey);
        ecsS3Config.setEnableVHosts(useVHost);
        ecsS3Config.setBucketName(bucket);

        TestConfig testConfig = new TestConfig();
        testConfig.setObjectOwner(accessKey);
        testConfig.setValidGroups(validGroups);
        testConfig.setValidPermissions(validPermissions);

        try {
            multiEndToEndTest(ecsS3Config, testConfig, true);
        } finally {
            try {
                s3.deleteBucket(bucket);
            } catch (Throwable t) {
                log.warn("could not delete bucket", t);
            }
        }
    }

    @Test
    public void testS3() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();
        final String bucket = "ecs-sync-s3-test-bucket";
        final String endpoint = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_SECRET_KEY);
        Assume.assumeNotNull(endpoint, accessKey, secretKey);
        URI endpointUri = new URI(endpoint);

        ClientConfiguration config = new ClientConfiguration().withSignerOverride("S3SignerType");
        AmazonS3Client s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), config);
        s3.setEndpoint(endpoint);
        try {
            s3.createBucket(bucket);
        } catch (AmazonServiceException e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        // for testing ACLs
        String authUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
        String everyone = "http://acs.amazonaws.com/groups/global/AllUsers";
        String[] validGroups = {authUsers, everyone};
        String[] validPermissions = {"READ", "WRITE", "FULL_CONTROL"};

        AwsS3Config awsS3Config = new AwsS3Config();
        if (endpointUri.getScheme() != null)
            awsS3Config.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
        awsS3Config.setHost(endpointUri.getHost());
        awsS3Config.setPort(endpointUri.getPort());
        awsS3Config.setAccessKey(accessKey);
        awsS3Config.setSecretKey(secretKey);
        awsS3Config.setLegacySignatures(true);
        awsS3Config.setDisableVHosts(true);
        awsS3Config.setBucketName(bucket);

        TestConfig testConfig = new TestConfig();
        testConfig.setObjectOwner(accessKey);
        testConfig.setValidGroups(validGroups);
        testConfig.setValidPermissions(validPermissions);

        try {
            multiEndToEndTest(awsS3Config, testConfig, true);
        } finally {
            try {
                s3.deleteBucket(bucket);
            } catch (Throwable t) {
                log.warn("could not delete bucket", t);
            }
        }
    }

    private void multiEndToEndTest(Object storageConfig, TestConfig testConfig, boolean syncAcl) {
        multiEndToEndTest(storageConfig, testConfig, null, syncAcl);
    }
    @SuppressWarnings("unchecked")
    private void multiEndToEndTest(Object storageConfig, TestConfig testConfig, ObjectAcl aclTemplate, boolean syncAcl) {
        if (testConfig == null) testConfig = new TestConfig();
        testConfig.withReadData(true).withDiscardData(false);

        // large objects
        testConfig.withObjectCount(LG_OBJ_COUNT).withMaxSize(LG_OBJ_MAX_SIZE);
        endToEndTest(storageConfig, testConfig, aclTemplate, syncAcl);

        // small objects
        testConfig.withObjectCount(SM_OBJ_COUNT).withMaxSize(SM_OBJ_MAX_SIZE);
        endToEndTest(storageConfig, testConfig, aclTemplate, syncAcl);
    }

    private <C> void endToEndTest(C storageConfig, TestConfig testConfig, ObjectAcl aclTemplate, boolean syncAcl) {
        SyncOptions options = new SyncOptions().withThreadCount(SYNC_THREAD_COUNT).withLogLevel(LogLevel.verbose);
        options.withSyncAcl(syncAcl).withTimingsEnabled(true).withTimingWindow(100);

        // create test source
        TestStorage testSource = new TestStorage();
        testSource.withAclTemplate(aclTemplate).withConfig(testConfig).withOptions(options);

        try {
            // send test data to test system
            options.setVerify(true);
            EcsSync sync = new EcsSync();
            sync.setSource(testSource); // must use the same source for consistency
            sync.setSyncConfig(new SyncConfig().withTarget(storageConfig).withOptions(options));
            sync.setPerfReportSeconds(2);
            sync.run();
            options.setVerify(false); // revert options

            Assert.assertEquals(0, sync.getStats().getObjectsFailed());

            // test verify-only in target
            options.setVerifyOnly(true);
            sync = new EcsSync();
            sync.setSource(testSource); // must use the same source for consistency
            sync.setSyncConfig(new SyncConfig().withTarget(storageConfig).withOptions(options));
            sync.setPerfReportSeconds(2);
            sync.run();
            options.setVerifyOnly(false); // revert options

            Assert.assertEquals(0, sync.getStats().getObjectsFailed());

            // read data from same system
            options.setVerify(true);
            sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig().withSource(storageConfig).withTarget(testConfig).withOptions(options));
            sync.setPerfReportSeconds(2);
            sync.setDbService(dbService);
            sync.run();
            options.setVerify(false); // revert options

            // save test target for verify-only
            TestStorage testTarget = (TestStorage) sync.getTarget();

            Assert.assertEquals(0, sync.getStats().getObjectsFailed());
            verifyDb(testSource);
            Assert.assertEquals(sync.getStats().getObjectsComplete(), sync.getEstimatedTotalObjects());
            Assert.assertEquals(sync.getStats().getBytesComplete(), sync.getEstimatedTotalBytes());

            // test verify-only in source
            options.setVerifyOnly(true);
            sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig().withSource(storageConfig).withOptions(options));
            sync.setTarget(testTarget);
            sync.setPerfReportSeconds(2);
            sync.setDbService(dbService);
            sync.run();
            options.setVerifyOnly(false); // revert options

            Assert.assertEquals(0, sync.getStats().getObjectsFailed());
            verifyDb(testSource);

            VerifyTest.verifyObjects(testSource, testSource.getRootObjects(), testTarget, testTarget.getRootObjects(), syncAcl);
        } finally {
            try {
                // delete the objects from the test system
                SyncStorage<C> storage = PluginUtil.newStorageFromConfig(storageConfig, options);
                storage.configure(storage, Collections.<SyncFilter>emptyIterator(), null);
                List<Future> futures = new ArrayList<>();
                for (ObjectSummary summary : storage.allObjects()) {
                    futures.add(recursiveDelete(storage, summary));
                }
                for (Future future : futures) {
                    try {
                        future.get();
                    } catch (Throwable t) {
                        log.warn("error deleting object", t);
                    }
                }
            } catch (Throwable t) {
                log.warn("could not delete objects after sync: " + t.getMessage());
            }
        }
    }

    private Future recursiveDelete(final SyncStorage<?> storage, final ObjectSummary object) throws ExecutionException, InterruptedException {
        final List<Future> futures = new ArrayList<>();
        if (object.isDirectory()) {
            for (ObjectSummary child : storage.children(object)) {
                futures.add(recursiveDelete(storage, child));
            }
        }
        return service.submit(new Callable() {
            @Override
            public Object call() throws Exception {
                for (Future future : futures) {
                    future.get();
                }
                try {
                    log.info("deleting {}", object.getIdentifier());
                    storage.delete(object.getIdentifier());
                } catch (Throwable t) {
                    log.warn("could not delete " + object.getIdentifier(), t);
                }
                return null;
            }
        });
    }

    private void verifyDb(TestStorage storage) {
        JdbcTemplate jdbcTemplate = dbService.getJdbcTemplate();

        long totalCount = verifyDbObjects(jdbcTemplate, storage, storage.getRootObjects());

        SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT count(target_id) FROM " + AbstractDbService.DEFAULT_OBJECTS_TABLE_NAME + " WHERE target_id != ''");
        Assert.assertTrue(rowSet.next());
        Assert.assertEquals(totalCount, rowSet.getLong(1));
        jdbcTemplate.update("DELETE FROM " + AbstractDbService.DEFAULT_OBJECTS_TABLE_NAME);
    }

    private long verifyDbObjects(JdbcTemplate jdbcTemplate, TestStorage storage, List<? extends SyncObject> objects) {
        Date now = new Date();
        long count = 0;
        for (SyncObject object : objects) {
            count++;
            String identifier = storage.getIdentifier(object.getRelativePath(), object.getMetadata().isDirectory());
            SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM " + AbstractDbService.DEFAULT_OBJECTS_TABLE_NAME + " WHERE target_id=?",
                    identifier);
            Assert.assertTrue(rowSet.next());
            Assert.assertEquals(identifier, rowSet.getString("target_id"));
            Assert.assertEquals(object.getMetadata().isDirectory(), rowSet.getBoolean("is_directory"));
            Assert.assertEquals(object.getMetadata().getContentLength(), rowSet.getLong("size"));
            // mtime in the DB is actually pulled from the target system, so we don't know what precision it will be in
            // or if the target system's clock is in sync, but let's assume it will always be within 5 minutes
            Assert.assertTrue(Math.abs(object.getMetadata().getModificationTime().getTime() - rowSet.getLong("mtime")) < 5 * 60 * 1000);
            Assert.assertEquals(ObjectStatus.Verified.getValue(), rowSet.getString("status"));
            long transferStart = rowSet.getLong("transfer_start"), transferComplete = rowSet.getLong("transfer_complete");
            if (transferStart > 0)
                Assert.assertTrue(now.getTime() - transferStart < 10 * 60 * 1000); // less than 10 minutes ago
            if (transferComplete > 0)
                Assert.assertTrue(now.getTime() - transferComplete < 10 * 60 * 1000); // less than 10 minutes ago
            Assert.assertTrue(now.getTime() - rowSet.getLong("verify_start") < 10 * 60 * 1000); // less than 10 minutes ago
            Assert.assertTrue(now.getTime() - rowSet.getLong("verify_complete") < 10 * 60 * 1000); // less than 10 minutes ago
            Assert.assertEquals(0, rowSet.getInt("retry_count"));
            if (object.getMetadata().isDirectory())
                count += verifyDbObjects(jdbcTemplate, storage, storage.getChildren(identifier));
        }
        return count;
    }
}

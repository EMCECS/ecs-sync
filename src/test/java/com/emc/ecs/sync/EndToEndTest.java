/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.emc.ecs.nfsclient.nfs.io.Nfs3File;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.emc.ecs.nfsclient.rpc.CredentialUnix;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.*;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.rest.LogLevel;
import com.emc.ecs.sync.service.SqliteDbService;
import com.emc.ecs.sync.service.SyncJobService;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.util.PluginUtil;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.sun.xml.bind.v2.TODO;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertFalse;

public class EndToEndTest {
    private static final Logger log = LoggerFactory.getLogger(EndToEndTest.class);

    private static final int SM_OBJ_COUNT = 200;
    private static final int SM_OBJ_MAX_SIZE = 10240; // 10K
    private static final int LG_OBJ_COUNT = 10;
    private static final int LG_OBJ_MAX_SIZE = 1024 * 1024; // 1M

    private static final int SYNC_THREAD_COUNT = 32;

    private ExecutorService service;

    private static class TestDbService extends SqliteDbService {
        TestDbService() {
            super(":memory:");
            initCheck();
        }

        void resetTable(String newTableName) {
            getJdbcTemplate().update("DROP TABLE IF EXISTS " + getObjectsTableName());
            setObjectsTableName(newTableName);
            createTable();
        }

        @Override
        public JdbcTemplate getJdbcTemplate() {
            return super.getJdbcTemplate();
        }
    }

    private final TestDbService dbService = new TestDbService();

    @Before
    public void before() {
        service = Executors.newFixedThreadPool(SYNC_THREAD_COUNT);
    }

    @After
    public void after() {
        if (service != null) service.shutdownNow();
    }

    @Test
    public void testTestPlugins() {
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
    public void testFilesystem() {
        try {
            Path tempDir = Files.createTempDirectory("ecs-sync-filesystem-test");

            FilesystemConfig filesystemConfig = new FilesystemConfig();
            filesystemConfig.setPath(tempDir.toAbsolutePath().toString());
            filesystemConfig.setStoreMetadata(true);

            multiEndToEndTest(filesystemConfig, new TestConfig(), false);

            Files.deleteIfExists(tempDir.resolve(ObjectMetadata.METADATA_DIR));
            Files.deleteIfExists(tempDir);
        } catch (IOException e) {
            throw new RuntimeException("problem with temp dir", e);
        }
    }

    @Test
    public void testNfs() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();
        String export = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_NFS_EXPORT);
        Assume.assumeNotNull(export);
        if (!export.contains(":")) throw new RuntimeException("invalid export: " + export);
        String server = export.split(":")[0];
        String mountPath = export.split(":")[1];

        final Nfs3 nfs = new Nfs3(server, mountPath, new CredentialUnix(0, 0, null), 3);
        final Nfs3File tempDir = new Nfs3File(nfs, "/ecs-sync-nfs-test");
        tempDir.mkdir();
        if (!tempDir.exists() || !tempDir.isDirectory()) {
            throw new RuntimeException("unable to make temp dir");
        }

        try {
            NfsConfig config = new NfsConfig();
            config.setServer(server);
            config.setMountPath(mountPath);
            config.setSubPath(tempDir.getPath().substring(1));
            config.setStoreMetadata(true);

            multiEndToEndTest(config, new TestConfig(), false);
        } finally {
            try {
                Nfs3File metaFile = tempDir.getChildFile(ObjectMetadata.METADATA_DIR);
                if (metaFile.exists()) {
                    metaFile.delete();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            tempDir.delete();
            assertFalse(tempDir.exists());
        }
    }

    @Test
    public void testArchive() {
        final File archive = new File("/tmp/ecs-sync-archive-test.zip");
        if (archive.exists()) archive.delete();
        archive.deleteOnExit();

        ArchiveConfig archiveConfig = new ArchiveConfig();
        archiveConfig.setPath(archive.getPath());
        archiveConfig.setStoreMetadata(true);

        TestConfig testConfig = new TestConfig().withReadData(true).withDiscardData(false);
        testConfig.withObjectCount(LG_OBJ_COUNT).withMaxSize(LG_OBJ_MAX_SIZE);

        endToEndTest(archiveConfig, testConfig, null, false, "large object");
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
        atmosConfig.setHosts(hosts.toArray(new String[0]));
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
        final boolean useVHost = Boolean.parseBoolean(syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_VHOST));
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
        ecsS3Config.setPreserveDirectories(true);

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
        final String region = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_REGION);
        Assume.assumeNotNull(endpoint, accessKey, secretKey);
        URI endpointUri = new URI(endpoint);

        ClientConfiguration config = new ClientConfiguration().withSignerOverride("S3SignerType");
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        builder.withClientConfiguration(config);
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));

        AmazonS3 s3 = builder.build();
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
        awsS3Config.setRegion(region);
        awsS3Config.setLegacySignatures(true);
        awsS3Config.setDisableVHosts(true);
        awsS3Config.setBucketName(bucket);
        awsS3Config.setPreserveDirectories(true);

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

    @Test
    public void testAzureBlob() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();

        final String connectString = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_AZURE_BLOB_CONNECT_STRING);
        Assume.assumeNotNull(connectString);
        if (!connectString.contains(";")) throw new RuntimeException("invalid export: " + connectString);

        AzureBlobConfig azureBlobConfig = new AzureBlobConfig();
        azureBlobConfig.setConnectionString(connectString);
        azureBlobConfig.setContainerName("azure-ecs-test");
        azureBlobConfig.setIncludeSnapShots(true);
        // TODO: need to implement the EndToEndTest after finish the: https://asdjira.isus.emc.com:8443/browse/ES-98
//        endToEndTest(azureBlobConfig, new TestConfig(), null, false);
    }

    private void multiEndToEndTest(Object storageConfig, TestConfig testConfig, boolean syncAcl) {
        multiEndToEndTest(storageConfig, testConfig, null, syncAcl);
    }

    private void multiEndToEndTest(Object storageConfig, TestConfig testConfig, ObjectAcl aclTemplate, boolean syncAcl) {
        if (testConfig == null) testConfig = new TestConfig();
        testConfig.withReadData(true).withDiscardData(false);

        // large objects
        String testName = "large objects";
        testConfig.withObjectCount(LG_OBJ_COUNT).withMaxSize(LG_OBJ_MAX_SIZE);
        endToEndTest(storageConfig, testConfig, aclTemplate, syncAcl, testName);

        // small objects
        testName = "small objects";
        testConfig.withObjectCount(SM_OBJ_COUNT).withMaxSize(SM_OBJ_MAX_SIZE);
        endToEndTest(storageConfig, testConfig, aclTemplate, syncAcl, testName);

        testName = "zero-byte objects";
        // zero-byte objects (always important!)
        testConfig.withObjectCount(SM_OBJ_COUNT).withMaxSize(0);
        endToEndTest(storageConfig, testConfig, aclTemplate, syncAcl, testName);
    }

    private void endToEndTest(Object storageConfig, TestConfig testConfig, ObjectAcl aclTemplate, boolean syncAcl, String testName) {
        SyncJobService.getInstance().setLogLevel(LogLevel.verbose);
        SyncOptions options = new SyncOptions().withThreadCount(SYNC_THREAD_COUNT);
        options.withSyncAcl(syncAcl).withTimingsEnabled(true).withTimingWindow(100);
        options.setRememberFailed(true);

        // set up DB table
        String tableName = "t" + System.currentTimeMillis();
        log.info("generated DB table name is {}", tableName);
        options.withDbTable(tableName);
        dbService.resetTable(tableName);

        // create test source
        TestStorage testSource = new TestStorage();
        testSource.withAclTemplate(aclTemplate).withConfig(testConfig).withOptions(options);

        try {
            // send test data to test system
            String jobName = testName + " - sync+verify to target";
            options.setVerify(true);
            EcsSync sync = new EcsSync();
            sync.setSource(testSource); // must use the same source for consistency
            sync.setSyncConfig(new SyncConfig().withTarget(storageConfig).withOptions(options));
            sync.setPerfReportSeconds(2);
            sync.run();
            options.setVerify(false); // revert options

            String summary = summarizeFailure(jobName, sync);
            Assert.assertEquals(summary, 0, sync.getStats().getObjectsFailed());

            // test verify-only in target
            jobName = testName + " - verify-only in target";
            options.setVerifyOnly(true);
            sync = new EcsSync();
            sync.setSource(testSource); // must use the same source for consistency
            sync.setSyncConfig(new SyncConfig().withTarget(storageConfig).withOptions(options));
            sync.setPerfReportSeconds(2);
            sync.run();
            options.setVerifyOnly(false); // revert options

            summary = summarizeFailure(jobName, sync);
            Assert.assertEquals(summary, 0, sync.getStats().getObjectsFailed());

            // read data from same system
            jobName = testName + " - read+verify from source";
            options.setVerify(true);
            sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig().withSource(storageConfig).withTarget(testConfig).withOptions(options));
            sync.setPerfReportSeconds(2);
            sync.setDbService(dbService);
            sync.run();
            options.setVerify(false); // revert options

            // save test target for verify-only
            TestStorage testTarget = (TestStorage) sync.getTarget();

            summary = summarizeFailure(jobName, sync);
            Assert.assertEquals(summary, 0, sync.getStats().getObjectsFailed());
            verifyDb(testSource);
            Assert.assertEquals(summary, sync.getStats().getObjectsComplete(), sync.getEstimatedTotalObjects());
            Assert.assertEquals(summary, sync.getStats().getBytesComplete(), sync.getEstimatedTotalBytes());

            // test verify-only in source
            jobName = testName + " - verify-only in source";
            options.setVerifyOnly(true);
            sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig().withSource(storageConfig).withOptions(options));
            sync.setTarget(testTarget);
            sync.setPerfReportSeconds(2);
            sync.setDbService(dbService);
            sync.run();
            options.setVerifyOnly(false); // revert options

            summary = summarizeFailure(jobName, sync);
            Assert.assertEquals(summary, 0, sync.getStats().getObjectsFailed());
            verifyDb(testSource);

            VerifyTest.verifyObjects(testSource, testSource.getRootObjects(), testTarget, testTarget.getRootObjects(), syncAcl);

            // test list-file operation
            jobName = testName + " - read+verify+list-file from source";
            File listFile = createListFile(sync.getSource()); // should be the real storage plugin (not test)
            options.setSourceListFile(listFile.getPath());
            sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig().withSource(storageConfig).withTarget(testConfig).withOptions(options));
            sync.run();
            options.setSourceListFile(null); // revert options

            summary = summarizeFailure(jobName, sync);
            Assert.assertEquals(summary, 0, sync.getStats().getObjectsFailed());
            Assert.assertEquals(summary, 0, sync.getStats().getObjectsFailed());

            testTarget = (TestStorage) sync.getTarget();

            VerifyTest.verifyObjects(testSource, testSource.getRootObjects(), testTarget, testTarget.getRootObjects(), syncAcl);
        } finally {
            try {
                // delete the objects from the test system
                SyncStorage<?> storage = PluginUtil.newStorageFromConfig(storageConfig, options);
                storage.configure(storage, Collections.emptyIterator(), null);
                List<Future<?>> futures = new ArrayList<>();
                for (ObjectSummary summary : storage.allObjects()) {
                    futures.add(recursiveDelete(storage, summary));
                }
                for (Future<?> future : futures) {
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

    private String summarizeFailure(String jobName, EcsSync sync) {
        String summary = "job " + jobName + " failed:\n";
        for (String failedObject : sync.getStats().getFailedObjects()) {
            summary += failedObject + "\n";
        }
        return summary;
    }

    private File createListFile(SyncStorage<?> storage) {
        try {
            File listFile = File.createTempFile("list-file", null);
            listFile.deleteOnExit();

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(listFile))) {
                listIdentifiers(writer, storage, storage.allObjects());
                writer.flush();
            }

            return listFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void listIdentifiers(BufferedWriter writer, SyncStorage<?> storage, Iterable<ObjectSummary> summaries) throws IOException {
        for (ObjectSummary summary : summaries) {
            writer.append(summary.getIdentifier()).append("\n");
            if (summary.isDirectory()) listIdentifiers(writer, storage, storage.children(summary));
        }
    }

    private Future<?> recursiveDelete(final SyncStorage<?> storage, final ObjectSummary object) {
        final List<Future<?>> futures = new ArrayList<>();
        if (object.isDirectory()) {
            for (ObjectSummary child : storage.children(object)) {
                futures.add(recursiveDelete(storage, child));
            }
        }
        return service.submit(() -> {
            for (Future<?> future : futures) {
                future.get();
            }
            try {
                log.info("deleting {}", object.getIdentifier());
                storage.delete(object.getIdentifier());
            } catch (Throwable t) {
                log.warn("could not delete " + object.getIdentifier(), t);
            }
            return null;
        });
    }

    private void verifyDb(TestStorage storage) {
        log.info("verifying test storage against database {}", storage.getOptions().getDbTable());

        JdbcTemplate jdbcTemplate = dbService.getJdbcTemplate();

        long totalCount = verifyDbObjects(jdbcTemplate, storage, storage.getRootObjects());

        SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT count(target_id) FROM " + storage.getOptions().getDbTable() + " WHERE target_id != ''");
        Assert.assertTrue(rowSet.next());
        Assert.assertEquals(totalCount, rowSet.getLong(1));
    }

    private long verifyDbObjects(JdbcTemplate jdbcTemplate, TestStorage storage, Collection<? extends SyncObject> objects) {
        Date now = new Date();
        long count = 0;
        for (SyncObject object : objects) {
            count++;
            String identifier = storage.getIdentifier(object.getRelativePath(), object.getMetadata().isDirectory());
            SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM " + storage.getOptions().getDbTable() + " WHERE target_id=?",
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

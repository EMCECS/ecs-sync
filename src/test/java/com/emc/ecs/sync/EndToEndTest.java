/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
import com.emc.atmos.api.AtmosApi;
import com.emc.atmos.api.AtmosConfig;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.ecs.sync.model.ObjectStatus;
import com.emc.ecs.sync.model.SyncAcl;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.*;
import com.emc.ecs.sync.service.DbService;
import com.emc.ecs.sync.service.SqliteDbService;
import com.emc.ecs.sync.source.*;
import com.emc.ecs.sync.target.*;
import com.emc.ecs.sync.test.SyncConfig;
import com.emc.ecs.sync.test.TestObjectSource;
import com.emc.ecs.sync.test.TestObjectTarget;
import com.emc.ecs.sync.test.TestSyncObject;
import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.rest.smart.ecs.Vdc;
import net.java.truevfs.access.TFile;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class EndToEndTest {
    Logger log = LoggerFactory.getLogger(EndToEndTest.class);

    private static final int SM_OBJ_COUNT = 200;
    private static final int SM_OBJ_MAX_SIZE = 10240; // 10K
    private static final int LG_OBJ_COUNT = 10;
    private static final int LG_OBJ_MAX_SIZE = 1024 * 1024; // 1M

    private static final int SYNC_THREAD_COUNT = 32;

    private static final ExecutorService service = Executors.newFixedThreadPool(SYNC_THREAD_COUNT);

    private File dbFile;

    @Before
    public void createDbFile() throws IOException {
        dbFile = File.createTempFile("sync-test-db", null);
        dbFile.deleteOnExit();
    }

    @Test
    public void testTestPlugins() throws Exception {
        TestObjectSource source = new TestObjectSource(SM_OBJ_COUNT, SM_OBJ_MAX_SIZE, null);

        TestObjectTarget target = new TestObjectTarget();

        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.run();

        List<TestSyncObject> targetObjects = target.getRootObjects();
        verifyObjects(source.getObjects(), targetObjects);
    }

    @Test
    public void testFilesystem() throws Exception {
        final File tempDir = new File("/tmp/ecs-sync-filesystem-test"); // File.createTempFile("ecs-sync-filesystem-test", "dir");
        tempDir.mkdir();
        tempDir.deleteOnExit();

        if (!tempDir.exists() || !tempDir.isDirectory())
            throw new RuntimeException("unable to make temp dir");

        PluginGenerator fsGenerator = new PluginGenerator(null) {
            @Override
            public SyncSource<?> createSource() {
                FilesystemSource source = new FilesystemSource();
                source.setRootFile(tempDir);
                return source;
            }

            @Override
            public SyncTarget createTarget() {
                FilesystemTarget target = new FilesystemTarget();
                target.setTargetRoot(tempDir);
                return target;
            }

            @Override
            public boolean isEstimator() {
                return true;
            }
        };

        endToEndTest(fsGenerator);
        new File(tempDir, SyncMetadata.METADATA_DIR).delete(); // delete this so the temp dir can go away
    }

    @Test
    public void testArchive() throws Exception {
        final File archive = new File("/tmp/ecs-sync-archive-test.zip");
        if (archive.exists()) archive.delete();
        archive.deleteOnExit();

        PluginGenerator<FileSyncObject> archiveGenerator = new PluginGenerator<FileSyncObject>(null) {
            @Override
            public SyncSource<FileSyncObject> createSource() {
                ArchiveFileSource source = new ArchiveFileSource();
                source.setRootFile(new TFile(archive));
                return source;
            }

            @Override
            public SyncTarget createTarget() {
                ArchiveFileTarget target = new ArchiveFileTarget();
                target.setTargetRoot(new TFile(archive));
                return target;
            }
        };

        endToEndTest(new TestObjectSource(LG_OBJ_COUNT, LG_OBJ_MAX_SIZE, null), archiveGenerator);
    }

    @Test
    public void testAtmos() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
        final String rootPath = "/ecs-sync-atmos-test/";
        String endpoints = syncProperties.getProperty(SyncConfig.PROP_ATMOS_ENDPOINTS);
        String uid = syncProperties.getProperty(SyncConfig.PROP_ATMOS_UID);
        String secretKey = syncProperties.getProperty(SyncConfig.PROP_ATMOS_SECRET);
        Assume.assumeNotNull(endpoints, uid, secretKey);

        List<URI> uris = new ArrayList<>();
        for (String endpoint : endpoints.split(",")) {
            uris.add(new URI(endpoint));
        }
        final AtmosApi atmos = new AtmosApiClient(new AtmosConfig(uid, secretKey, uris.toArray(new URI[uris.size()])));

        List<String> validGroups = Collections.singletonList("other");
        List<String> validPermissions = Arrays.asList("READ", "WRITE", "FULL_CONTROL");
        SyncAcl template = new SyncAcl();
        template.addGroupGrant("other", "NONE");

        PluginGenerator<AtmosSyncObject> atmosGenerator = new PluginGenerator<AtmosSyncObject>(uid.substring(uid.lastIndexOf("/") + 1)) {
            @Override
            public SyncSource<AtmosSyncObject> createSource() {
                AtmosSource source = new AtmosSource();
                source.setAtmos(atmos);
                source.setNamespaceRoot(rootPath);
                source.setIncludeAcl(true);
                return source;
            }

            @Override
            public SyncTarget createTarget() {
                AtmosTarget target = new AtmosTarget();
                target.setAtmos(atmos);
                target.setDestNamespace(rootPath);
                target.setIncludeAcl(true);
                return target;
            }
        }.withAclTemplate(template).withValidGroups(validGroups).withValidPermissions(validPermissions);

        endToEndTest(atmosGenerator);
    }

    @Test
    public void testEcsS3() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
        final String bucket = "ecs-sync-ecs-s3-test-bucket";
        String endpoint = syncProperties.getProperty(SyncConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(SyncConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(SyncConfig.PROP_S3_SECRET_KEY);
        final boolean useVHost = Boolean.valueOf(syncProperties.getProperty(SyncConfig.PROP_S3_VHOST));
        Assume.assumeNotNull(endpoint, accessKey, secretKey);
        final URI endpointUri = new URI(endpoint);

        S3Config s3Config;
        if (useVHost) s3Config = new S3Config(endpointUri);
        else s3Config = new S3Config(Protocol.valueOf(endpointUri.getScheme().toUpperCase()), endpointUri.getHost());
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
        List<String> validGroups = Arrays.asList(authUsers, everyone);
        List<String> validPermissions = Arrays.asList("READ", "WRITE", "FULL_CONTROL");

        PluginGenerator<EcsS3SyncObject> s3Generator = new PluginGenerator<EcsS3SyncObject>(accessKey) {
            @Override
            public SyncSource<EcsS3SyncObject> createSource() {
                EcsS3Source source = new EcsS3Source();
                source.setEndpoint(endpointUri);
                source.setProtocol(endpointUri.getScheme());
                source.setVdcs(Collections.singletonList(new Vdc(endpointUri.getHost())));
                source.setPort(endpointUri.getPort());
                source.setEnableVHosts(useVHost);
                source.setAccessKey(accessKey);
                source.setSecretKey(secretKey);
                source.setBucketName(bucket);
                source.setIncludeAcl(true);
                return source;
            }

            @Override
            public SyncTarget createTarget() {
                EcsS3Target target = new EcsS3Target();
                target.setEndpoint(endpointUri);
                target.setProtocol(endpointUri.getScheme());
                target.setVdcs(Collections.singletonList(new Vdc(endpointUri.getHost())));
                target.setPort(endpointUri.getPort());
                target.setEnableVHosts(useVHost);
                target.setAccessKey(accessKey);
                target.setSecretKey(secretKey);
                target.setBucketName(bucket);
                target.setIncludeAcl(true);
                return target;
            }

            @Override
            public boolean isEstimator() {
                return true;
            }
        }.withValidGroups(validGroups).withValidPermissions(validPermissions);

        try {
            endToEndTest(s3Generator);
        } finally {
            try {
                s3.deleteBucket(bucket);
            } catch (Throwable t) {
                log.warn("could not delete bucket: " + t.getMessage());
            }
        }
    }

    @Test
    public void testS3() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
        final String bucket = "ecs-sync-s3-test-bucket";
        final String endpoint = syncProperties.getProperty(SyncConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(SyncConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(SyncConfig.PROP_S3_SECRET_KEY);
        Assume.assumeNotNull(endpoint, accessKey, secretKey);

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
        List<String> validGroups = Arrays.asList(authUsers, everyone);
        List<String> validPermissions = Arrays.asList("READ", "WRITE", "FULL_CONTROL");

        PluginGenerator<S3SyncObject> s3Generator = new PluginGenerator<S3SyncObject>(accessKey) {
            @Override
            public SyncSource<S3SyncObject> createSource() {
                S3Source source = new S3Source();
                source.setEndpoint(endpoint);
                source.setAccessKey(accessKey);
                source.setSecretKey(secretKey);
                source.setLegacySignatures(true);
                source.setDisableVHosts(true);
                source.setBucketName(bucket);
                source.setIncludeAcl(true);
                return source;
            }

            @Override
            public SyncTarget createTarget() {
                S3Target target = new S3Target();
                target.setEndpoint(endpoint);
                target.setAccessKey(accessKey);
                target.setSecretKey(secretKey);
                target.setLegacySignatures(true);
                target.setDisableVHosts(true);
                target.setBucketName(bucket);
                target.setIncludeAcl(true);
                return target;
            }
        }.withValidGroups(validGroups).withValidPermissions(validPermissions);

        try {
            endToEndTest(s3Generator);
        } finally {
            try {
                s3.deleteBucket(bucket);
            } catch (Throwable t) {
                log.warn("could not delete bucket: " + t.getMessage());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void endToEndTest(PluginGenerator generator) {

        // large objects
        TestObjectSource testSource = new TestObjectSource(LG_OBJ_COUNT, LG_OBJ_MAX_SIZE, generator.getObjectOwner(),
                generator.getAclTemplate(), generator.getValidUsers(), generator.getValidGroups(), generator.getValidPermissions());
        endToEndTest(testSource, generator);

        // small objects
        testSource = new TestObjectSource(SM_OBJ_COUNT, SM_OBJ_MAX_SIZE, generator.getObjectOwner(),
                generator.getAclTemplate(), generator.getValidUsers(), generator.getValidGroups(), generator.getValidPermissions());
        endToEndTest(testSource, generator);
    }

    private <T extends SyncObject> void endToEndTest(TestObjectSource testSource, PluginGenerator<T> generator) {
        try {
            DbService dbService = new SqliteDbService(dbFile.getPath());

            // send test data to test system
            EcsSync sync = new EcsSync();
            sync.setSource(testSource);
            sync.setTarget(generator.createTarget());
            sync.setSyncThreadCount(SYNC_THREAD_COUNT);
            sync.setVerify(true);
            sync.setReportPerformance(2);
            sync.run();

            Assert.assertEquals(0, sync.getObjectsFailed());

            // test verify-only in target
            SyncTarget target = generator.createTarget();
            target.setMonitorPerformance(true);
            sync = new EcsSync();
            sync.setSource(testSource);
            sync.setTarget(target);
            sync.setSyncThreadCount(SYNC_THREAD_COUNT);
            sync.setVerifyOnly(true);
            sync.setReportPerformance(2);
            sync.run();

            Assert.assertEquals(0, sync.getObjectsFailed());

            // read data from same system
            TestObjectTarget testTarget = new TestObjectTarget();
            sync = new EcsSync();
            sync.setDbService(dbService);
            sync.setReprocessObjects(true);
            sync.setSource(generator.createSource());
            sync.setTarget(testTarget);
            sync.setSyncThreadCount(SYNC_THREAD_COUNT);
            sync.setVerify(true);
            sync.setReportPerformance(2);
            sync.run();

            Assert.assertEquals(0, sync.getObjectsFailed());
            verifyDb(testSource, false);
            if (generator.isEstimator()) {
                Assert.assertEquals(sync.getObjectsComplete(), sync.getEstimatedTotalObjects());
                Assert.assertEquals(sync.getBytesComplete(), sync.getEstimatedTotalBytes());
            }

            // test verify-only in source
            SyncSource source = generator.createSource();
            source.setMonitorPerformance(true);
            sync = new EcsSync();
            sync.setDbService(dbService);
            sync.setReprocessObjects(true);
            sync.setSource(source);
            sync.setTarget(testTarget);
            sync.setSyncThreadCount(SYNC_THREAD_COUNT);
            sync.setVerifyOnly(true);
            sync.setReportPerformance(2);
            sync.run();

            Assert.assertEquals(0, sync.getObjectsFailed());
            verifyDb(testSource, true);

            verifyObjects(testSource.getObjects(), testTarget.getRootObjects());
        } finally {
            try {
                // delete the objects from the test system
                SyncSource<T> source = generator.createSource();
                source.configure(source, null, null);
                recursiveDelete(source, source.iterator());
            } catch (Throwable t) {
                log.warn("could not delete objects after sync: " + t.getMessage());
            }
        }
    }

    private <T extends SyncObject> void recursiveDelete(final SyncSource<T> source, Iterator<T> objects) throws ExecutionException, InterruptedException {
        List<Future> futures = new ArrayList<>();
        while (objects.hasNext()) {
            final T syncObject = objects.next();
            if (syncObject.isDirectory()) {
                recursiveDelete(source, source.childIterator(syncObject));
            }
            futures.add(service.submit(new Runnable() {
                @Override
                public void run() {
                    source.delete(syncObject);
                }
            }));
        }
        for (Future future : futures) {
            future.get();
        }
    }

    public static void verifyObjects(List<TestSyncObject> sourceObjects, List<TestSyncObject> targetObjects) {
        for (TestSyncObject sourceObject : sourceObjects) {
            String currentPath = sourceObject.getRelativePath();
            Assert.assertTrue(currentPath + " - missing from target", targetObjects.contains(sourceObject));
            for (TestSyncObject targetObject : targetObjects) {
                if (sourceObject.getRelativePath().equals(targetObject.getRelativePath())) {
                    verifyMetadata(sourceObject.getMetadata(), targetObject.getMetadata(), currentPath);
                    if (sourceObject.isDirectory()) {
                        Assert.assertTrue(currentPath + " - source is directory but target is not", targetObject.isDirectory());
                        verifyObjects(sourceObject.getChildren(), targetObject.getChildren());
                    } else {
                        Assert.assertFalse(currentPath + " - source is data object but target is not", targetObject.isDirectory());
                        Assert.assertEquals(currentPath + " - content-type different", sourceObject.getMetadata().getContentType(),
                                targetObject.getMetadata().getContentType());
                        Assert.assertEquals(currentPath + " - data size different", sourceObject.getMetadata().getContentLength(),
                                targetObject.getMetadata().getContentLength());
                        Assert.assertArrayEquals(currentPath + " - data not equal", sourceObject.getData(), targetObject.getData());
                    }
                }
            }
        }
    }

    public static void verifyMetadata(SyncMetadata sourceMetadata, SyncMetadata targetMetadata, String path) {
        if (sourceMetadata == null || targetMetadata == null)
            Assert.fail(String.format("%s - metadata can never be null (source: %s, target: %s)",
                    path, sourceMetadata, targetMetadata));

        // must be reasonable about mtime; we can't always set it on the target
        if (sourceMetadata.getModificationTime() == null)
            Assert.assertNull(path + " - source mtime is null, but target is not", targetMetadata.getModificationTime());
        else if (targetMetadata.getModificationTime() == null)
            Assert.fail(path + " - target mtime is null, but source is not");
        else
            Assert.assertTrue(path + " - target mtime is older",
                    sourceMetadata.getModificationTime().compareTo(targetMetadata.getModificationTime()) < 1000);
        Assert.assertEquals(path + " - different user metadata count", sourceMetadata.getUserMetadata().size(),
                targetMetadata.getUserMetadata().size());
        for (String key : sourceMetadata.getUserMetadata().keySet()) {
            Assert.assertEquals(path + " - meta[" + key + "] different", sourceMetadata.getUserMetadataValue(key).trim(),
                    targetMetadata.getUserMetadataValue(key).trim()); // some systems trim metadata values
        }

        verifyAcl(sourceMetadata.getAcl(), targetMetadata.getAcl());

        // not verifying system metadata here
    }

    public static void verifyAcl(SyncAcl sourceAcl, SyncAcl targetAcl) {
        // only verify ACL if it's set on the source
        if (sourceAcl != null) {
            Assert.assertNotNull(targetAcl);
            Assert.assertEquals(sourceAcl, targetAcl); // SyncAcl implements .equals()
        }
    }

    protected void verifyDb(TestObjectSource testSource, boolean truncateDb) {
        SingleConnectionDataSource ds = new SingleConnectionDataSource();
        ds.setUrl(SqliteDbService.JDBC_URL_BASE + dbFile.getPath());
        ds.setSuppressClose(true);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);

        long totalCount = verifyDbObjects(jdbcTemplate, testSource.getObjects());
        try {
            SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT count(source_id) FROM " + DbService.DEFAULT_OBJECTS_TABLE_NAME + " WHERE target_id != ''");
            Assert.assertTrue(rowSet.next());
            Assert.assertEquals(totalCount, rowSet.getLong(1));
            if (truncateDb) jdbcTemplate.update("DELETE FROM " + DbService.DEFAULT_OBJECTS_TABLE_NAME);
        } finally {
            try {
                ds.destroy();
            } catch (Throwable t) {
                log.warn("could not close datasource", t);
            }
        }
    }

    protected long verifyDbObjects(JdbcTemplate jdbcTemplate, List<TestSyncObject> objects) {
        Date now = new Date();
        long count = 0;
        for (TestSyncObject object : objects) {
            count++;
            SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM " + DbService.DEFAULT_OBJECTS_TABLE_NAME + " WHERE target_id=?",
                    object.getSourceIdentifier());
            Assert.assertTrue(rowSet.next());
            Assert.assertEquals(object.getSourceIdentifier(), rowSet.getString("target_id"));
            Assert.assertEquals(object.isDirectory(), rowSet.getBoolean("is_directory"));
            Assert.assertEquals(object.getMetadata().getContentLength(), rowSet.getLong("size"));
            // mtime in the DB is actually pulled from the target system, so we don't know what precision it will be in
            // or if the target system's clock is in sync, but let's assume it will always be within 5 minutes
            Assert.assertTrue(Math.abs(object.getMetadata().getModificationTime().getTime() - rowSet.getLong("mtime")) < 5 * 60 * 1000);
            Assert.assertEquals(ObjectStatus.Verified.getValue(), rowSet.getString("status"));
            Assert.assertTrue(now.getTime() - rowSet.getLong("transfer_start") < 10 * 60 * 1000); // less than 10 minutes ago
            Assert.assertTrue(now.getTime() - rowSet.getLong("transfer_complete") < 10 * 60 * 1000); // less than 10 minutes ago
            Assert.assertTrue(now.getTime() - rowSet.getLong("verify_start") < 10 * 60 * 1000); // less than 10 minutes ago
            Assert.assertTrue(now.getTime() - rowSet.getLong("verify_complete") < 10 * 60 * 1000); // less than 10 minutes ago
            Assert.assertEquals(object.getFailureCount(), rowSet.getInt("retry_count"));
            if (object.getFailureCount() > 0) {
                String error = rowSet.getString("error_message");
                Assert.assertNotNull(error);
                log.warn("{} was retried {} time{}; error: {}",
                        object.getRelativePath(), object.getFailureCount(), object.getFailureCount() > 1 ? "s" : "", error);
            }
            if (object.isDirectory())
                count += verifyDbObjects(jdbcTemplate, object.getChildren());
        }
        return count;
    }

    private abstract class PluginGenerator<T extends SyncObject> {
        private String objectOwner;
        private SyncAcl aclTemplate;
        private List<String> validUsers;
        private List<String> validGroups;
        private List<String> validPermissions;

        public PluginGenerator(String objectOwner) {
            this.objectOwner = objectOwner;
        }

        public abstract SyncSource<T> createSource();

        public abstract SyncTarget createTarget();

        /**
         * Override to test the accuracy of the totals estimation provided by the source plugin
         */
        public boolean isEstimator() {
            return false;
        }

        public String getObjectOwner() {
            return objectOwner;
        }

        public SyncAcl getAclTemplate() {
            return aclTemplate;
        }

        public void setAclTemplate(SyncAcl aclTemplate) {
            this.aclTemplate = aclTemplate;
        }

        public List<String> getValidUsers() {
            return validUsers;
        }

        public void setValidUsers(List<String> validUsers) {
            this.validUsers = validUsers;
        }

        public List<String> getValidGroups() {
            return validGroups;
        }

        public void setValidGroups(List<String> validGroups) {
            this.validGroups = validGroups;
        }

        public List<String> getValidPermissions() {
            return validPermissions;
        }

        public void setValidPermissions(List<String> validPermissions) {
            this.validPermissions = validPermissions;
        }

        public PluginGenerator<T> withAclTemplate(SyncAcl aclTemplate) {
            setAclTemplate(aclTemplate);
            return this;
        }

        public PluginGenerator<T> withValidUsers(List<String> validUsers) {
            setValidUsers(validUsers);
            return this;
        }

        public PluginGenerator<T> withValidGroups(List<String> validGroups) {
            setValidGroups(validGroups);
            return this;
        }

        public PluginGenerator<T> withValidPermissions(List<String> validPermissions) {
            setValidPermissions(validPermissions);
            return this;
        }
    }
}

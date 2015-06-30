/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.emc.atmos.api.AtmosApi;
import com.emc.atmos.api.AtmosConfig;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.vipr.sync.model.SyncAcl;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.object.S3SyncObject;
import com.emc.vipr.sync.model.object.SyncObject;
import com.emc.vipr.sync.source.*;
import com.emc.vipr.sync.target.*;
import com.emc.vipr.sync.test.SyncConfig;
import com.emc.vipr.sync.test.TestObjectSource;
import com.emc.vipr.sync.test.TestObjectTarget;
import com.emc.vipr.sync.test.TestSyncObject;
import net.java.truevfs.access.TFile;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class EndToEndTest {
    Logger l4j = Logger.getLogger(EndToEndTest.class);

    private static final int SM_OBJ_COUNT = 200;
    private static final int SM_OBJ_MAX_SIZE = 10240; // 10K
    private static final int LG_OBJ_COUNT = 10;
    private static final int LG_OBJ_MAX_SIZE = 1024 * 1024; // 1M

    private static final int SYNC_THREAD_COUNT = 16;

    private static final ExecutorService service = Executors.newFixedThreadPool(8);

    @Test
    public void testTestPlugins() throws Exception {
        TestObjectSource source = new TestObjectSource(SM_OBJ_COUNT, SM_OBJ_MAX_SIZE, null) {
//            @Override
//            public void delete(S3SyncObject syncObject) {
//
//            }
        };

        TestObjectTarget target = new TestObjectTarget();

        ViPRSync sync = new ViPRSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.run();

        List<TestSyncObject> targetObjects = target.getRootObjects();
        verifyObjects(source.getObjects(), targetObjects);
    }

    @Test
    public void testFilesystem() throws Exception {
        final File tempDir = new File("/tmp/vipr-sync-filesystem-test"); // File.createTempFile("vipr-sync-filesystem-test", "dir");
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
        };

        endToEndTest(fsGenerator, false);
        new File(tempDir, SyncMetadata.METADATA_DIR).delete(); // delete this so the temp dir can go away
    }

    @Test
    public void testArchive() throws Exception {
        final File archive = new File("/tmp/vipr-sync-archive-test.zip");
        if (archive.exists()) archive.delete();
        archive.deleteOnExit();

        PluginGenerator archiveGenerator = new PluginGenerator(null) {
            @Override
            public SyncSource<?> createSource() {
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

        endToEndTest(new TestObjectSource(LG_OBJ_COUNT, LG_OBJ_MAX_SIZE, null) {
//            @Override
//            public void delete(S3SyncObject syncObject) {
//
//            }
        }, archiveGenerator);
    }

    @Test
    public void testAtmos() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
        final String rootPath = "/vipr-sync-atmos-test/";
        String endpoints = syncProperties.getProperty(SyncConfig.PROP_ATMOS_ENDPOINTS);
        String uid = syncProperties.getProperty(SyncConfig.PROP_ATMOS_UID);
        String secretKey = syncProperties.getProperty(SyncConfig.PROP_ATMOS_SECRET);
        Assume.assumeNotNull(endpoints, uid, secretKey);

        List<URI> uris = new ArrayList<URI>();
        for (String endpoint : endpoints.split(",")) {
            uris.add(new URI(endpoint));
        }
        final AtmosApi atmos = new AtmosApiClient(new AtmosConfig(uid, secretKey, uris.toArray(new URI[uris.size()])));

        List<String> validGroups = Arrays.asList("other");
        List<String> validPermissions = Arrays.asList("READ", "WRITE", "FULL_CONTROL");

        PluginGenerator atmosGenerator = new PluginGenerator(uid.substring(uid.lastIndexOf("/") + 1)) {
            @Override
            public SyncSource<?> createSource() {
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
        }.withValidGroups(validGroups).withValidPermissions(validPermissions);

        endToEndTest(atmosGenerator, false);
    }

    @Test
    public void testS3() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
        final String bucket = "vipr-sync-s3-test-bucket";
        final String endpoint = syncProperties.getProperty(SyncConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(SyncConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(SyncConfig.PROP_S3_SECRET_KEY);
        Assume.assumeNotNull(endpoint, accessKey, secretKey);

        AmazonS3Client s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
        s3.setEndpoint(endpoint);
        try {
            s3.createBucket(bucket);
        } catch (AmazonServiceException e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        // for testing ACLs
        String authedUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
        String everyone = "http://acs.amazonaws.com/groups/global/AllUsers";
        List<String> validGroups = Arrays.asList(authedUsers, everyone);
        List<String> validPermissions = Arrays.asList("READ", "WRITE", "FULL_CONTROL");

        PluginGenerator s3Generator = new PluginGenerator(accessKey) {
            @Override
            public SyncSource<?> createSource() {
                S3Source source = new S3Source();
                source.setEndpoint(endpoint);
                source.setAccessKey(accessKey);
                source.setSecretKey(secretKey);
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
                target.setBucketName(bucket);
                target.setIncludeAcl(true);
                return target;
            }
        }.withValidGroups(validGroups).withValidPermissions(validPermissions);

        try {
            endToEndTest(s3Generator, true);
        } finally {
            try {
                s3.deleteBucket(bucket);
            } catch (Throwable t) {
                l4j.warn("could not delete bucket: " + t.getMessage());
            }
        }
    }

    private void endToEndTest(PluginGenerator generator, boolean pruneDirectories) {

        // large objects
        TestObjectSource testSource = new TestObjectSource(LG_OBJ_COUNT, LG_OBJ_MAX_SIZE, generator.getObjectOwner(),
                generator.getValidUsers(), generator.getValidGroups(), generator.getValidPermissions()) {
//            @Override
//            public void delete(S3SyncObject syncObject) {
//
//            }
        };
        if (pruneDirectories) pruneDirectories(testSource.getObjects());
        endToEndTest(testSource, generator);

        // small objects
        testSource = new TestObjectSource(SM_OBJ_COUNT, SM_OBJ_MAX_SIZE, generator.getObjectOwner(),
                generator.getValidUsers(), generator.getValidGroups(), generator.getValidPermissions()) {
//            @Override
//            public void delete(S3SyncObject syncObject) {
//
//            }
        };
        if (pruneDirectories) pruneDirectories(testSource.getObjects());
        endToEndTest(testSource, generator);
    }

    private <T extends SyncObject> void endToEndTest(TestObjectSource testSource, PluginGenerator<T> generator) {
        try {

            // send test data to test system
            ViPRSync sync = new ViPRSync();
            sync.setSource(testSource);
            sync.setTarget(generator.createTarget());
            sync.setSyncThreadCount(SYNC_THREAD_COUNT);
            sync.setVerify(true);
            sync.run();

            Assert.assertEquals(0, sync.getFailedCount());

            // test verify-only in target
            sync = new ViPRSync();
            sync.setSource(testSource);
            sync.setTarget(generator.createTarget());
            sync.setSyncThreadCount(SYNC_THREAD_COUNT);
            sync.setVerifyOnly(true);
            sync.run();

            Assert.assertEquals(0, sync.getFailedCount());

            // read data from same system
            TestObjectTarget testTarget = new TestObjectTarget();
            sync = new ViPRSync();
            sync.setSource(generator.createSource());
            sync.setTarget(testTarget);
            sync.setSyncThreadCount(SYNC_THREAD_COUNT);
            sync.setVerify(true);
            sync.run();

            Assert.assertEquals(0, sync.getFailedCount());

            // test verify-only in source
            sync = new ViPRSync();
            sync.setSource(generator.createSource());
            sync.setTarget(testTarget);
            sync.setSyncThreadCount(SYNC_THREAD_COUNT);
            sync.setVerifyOnly(true);
            sync.run();

            Assert.assertEquals(0, sync.getFailedCount());

            verifyObjects(testSource.getObjects(), testTarget.getRootObjects());
        } finally {
            try {
                // delete the objects from the test system
                SyncSource<T> source = generator.createSource();
                source.configure(source, null, null);
                for (T object : source) {
                    recursiveDelete(source, object).get(); // wait for root to be deleted
                }
            } catch (Throwable t) {
                l4j.warn("could not delete objects after sync: " + t.getMessage());
            }
        }
    }

    private void applyAcl(List<TestSyncObject> testObjects, SyncAcl acl) {
        for (TestSyncObject object : testObjects) {
            object.getMetadata().setAcl(acl);
            if (object.isDirectory()) applyAcl(object.getChildren(), acl);
        }
    }

    // some systems don't store directories (i.e. S3), so prune empty ones and remove all metadata
    private void pruneDirectories(List<TestSyncObject> testObjects) {
        for (Iterator<TestSyncObject> i = testObjects.iterator(); i.hasNext(); ) {
            TestSyncObject object = i.next();
            if (object.isDirectory()) {
                pruneDirectories(object.getChildren());
                if (object.getChildren().isEmpty()) i.remove();
                else object.setMetadata(new SyncMetadata()); // remove metadata
            }
        }
    }

    private <T extends SyncObject<T>> Future recursiveDelete(final SyncSource<T> source, final T syncObject) throws ExecutionException, InterruptedException {
        List<Future> futures = new ArrayList<Future>();
        if (syncObject.isDirectory()) {
            Iterator<T> i = source.childIterator(syncObject);
            while (i.hasNext()) {
                final T child = i.next();
                futures.add(recursiveDelete(source, child));
            }
        }
        for (Future future : futures) {
            future.get();
        }
        return service.submit(new Runnable() {
            @Override
            public void run() {
                source.delete(syncObject);
            }
        });
    }

    public static void verifyObjects(List<TestSyncObject> sourceObjects, List<TestSyncObject> targetObjects) {
        for (TestSyncObject sourceObject : sourceObjects) {
            String currentPath = sourceObject.getRelativePath();
            Assert.assertTrue(currentPath + " - missing from target", targetObjects.contains(sourceObject));
            for (TestSyncObject targetObject : targetObjects) {
                if (sourceObject.getRelativePath().equals(targetObject.getRelativePath())) {
                    Assert.assertEquals("relative paths not equal", sourceObject.getRelativePath(), targetObject.getRelativePath());
                    verifyMetadata(sourceObject.getMetadata(), targetObject.getMetadata(), currentPath);
                    if (sourceObject.isDirectory()) {
                        Assert.assertTrue(currentPath + " - source is directory but target is not", targetObject.isDirectory());
                        verifyObjects(sourceObject.getChildren(), targetObject.getChildren());
                    } else {
                        Assert.assertFalse(currentPath + " - source is data object but target is not", targetObject.isDirectory());
                        Assert.assertEquals(currentPath + " - content-type different", sourceObject.getMetadata().getContentType(),
                                targetObject.getMetadata().getContentType());
                        Assert.assertEquals(currentPath + " - data size different", sourceObject.getMetadata().getSize(),
                                targetObject.getMetadata().getSize());
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

    private abstract class PluginGenerator<T extends SyncObject> {
        private String objectOwner;
        private List<String> validUsers;
        private List<String> validGroups;
        private List<String> validPermissions;

        public PluginGenerator(String objectOwner) {
            this.objectOwner = objectOwner;
        }

        public abstract SyncSource<T> createSource();

        public abstract SyncTarget createTarget();

        public String getObjectOwner() {
            return objectOwner;
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

        public PluginGenerator withValidUsers(List<String> validUsers) {
            setValidUsers(validUsers);
            return this;
        }

        public PluginGenerator withValidGroups(List<String> validGroups) {
            setValidGroups(validGroups);
            return this;
        }

        public PluginGenerator withValidPermissions(List<String> validPermissions) {
            setValidPermissions(validPermissions);
            return this;
        }
    }
}

package com.emc.vipr.sync.test;

import com.emc.atmos.api.AtmosApi;
import com.emc.atmos.api.AtmosConfig;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.vipr.sync.ViPRSync;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.*;
import com.emc.vipr.sync.target.*;
import com.emc.vipr.sync.test.util.SyncConfig;
import com.emc.vipr.sync.test.util.TestObjectSource;
import com.emc.vipr.sync.test.util.TestObjectTarget;
import com.emc.vipr.sync.test.util.TestSyncObject;
import net.java.truevfs.access.TFile;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
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

    private static final int SYNC_THREAD_COUNT = 8;

    private static final ExecutorService service = Executors.newFixedThreadPool(8);

    @Test
    public void testTestPlugins() throws Exception {
        List<TestSyncObject> sourceObjects = TestObjectSource.generateRandomObjects(SM_OBJ_COUNT, SM_OBJ_MAX_SIZE);
        TestObjectSource source = new TestObjectSource(sourceObjects);

        TestObjectTarget target = new TestObjectTarget();

        ViPRSync sync = new ViPRSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.run();

        List<TestSyncObject> targetObjects = target.getRootObjects();
        verifyObjects(sourceObjects, targetObjects, "");
    }

    @Test
    public void testFilesystem() throws Exception {
        final File tempDir = new File("/tmp/vipr-sync-filesystem-test"); // File.createTempFile("vipr-sync-filesystem-test", "dir");
        tempDir.mkdir();
        tempDir.deleteOnExit();

        if (!tempDir.exists() || !tempDir.isDirectory())
            throw new RuntimeException("unable to make temp dir");

        PluginGenerator fsGenerator = new PluginGenerator() {
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

        endToEndTest(fsGenerator);
        new File(tempDir, SyncMetadata.METADATA_DIR).delete(); // delete this so the temp dir can go away
    }

    @Test
    public void testArchive() throws Exception {
        final File archive = new File("/tmp/vipr-sync-archive-test.zip");
        if (archive.exists()) archive.delete();
        archive.deleteOnExit();

        PluginGenerator archiveGenerator = new PluginGenerator() {
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

        endToEndTest(TestObjectSource.generateRandomObjects(LG_OBJ_COUNT, LG_OBJ_MAX_SIZE), archiveGenerator);
    }

    @Test
    public void testAtmos() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
        final String rootPath = "/vipr-sync-atmos-test/";
        String endpoints = syncProperties.getProperty(SyncConfig.PROP_ATMOS_ENDPOINTS);
        String uid = syncProperties.getProperty(SyncConfig.PROP_ATMOS_UID);
        String secretKey = syncProperties.getProperty(SyncConfig.PROP_ATMOS_SECRET);
        Assume.assumeNotNull(endpoints, uid, secretKey);

        List<URI> uris = new ArrayList<>();
        for (String endpoint : endpoints.split(",")) {
            uris.add(new URI(endpoint));
        }
        final AtmosApi atmos = new AtmosApiClient(new AtmosConfig(uid, secretKey, uris.toArray(new URI[uris.size()])));

        PluginGenerator atmosGenerator = new PluginGenerator() {
            @Override
            public SyncSource<?> createSource() {
                AtmosSource source = new AtmosSource();
                source.setAtmos(atmos);
                source.setNamespaceRoot(rootPath);
                return source;
            }

            @Override
            public SyncTarget createTarget() {
                AtmosTarget target = new AtmosTarget();
                target.setAtmos(atmos);
                target.setDestNamespace(rootPath);
                return target;
            }
        };

        endToEndTest(atmosGenerator);
    }

    @Test
    public void testS3() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
        final String bucket = "vipr-sync-s3-test-bucket";
        final String endpoint = syncProperties.getProperty(SyncConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(SyncConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(SyncConfig.PROP_S3_SECRET_KEY);
        Assume.assumeNotNull(endpoint, accessKey, secretKey);

        PluginGenerator s3Generator = new PluginGenerator() {
            @Override
            public SyncSource<?> createSource() {
                S3Source source = new S3Source();
                source.setEndpoint(endpoint);
                source.setAccessKey(accessKey);
                source.setSecretKey(secretKey);
                source.setBucketName(bucket);
                return source;
            }

            @Override
            public SyncTarget createTarget() {
                S3Target target = new S3Target();
                target.setEndpoint(endpoint);
                target.setAccessKey(accessKey);
                target.setSecretKey(secretKey);
                target.setBucketName(bucket);
                return target;
            }
        };

        endToEndTest(s3Generator);
    }

    private void endToEndTest(PluginGenerator generator) {
        // large objects
        endToEndTest(TestObjectSource.generateRandomObjects(LG_OBJ_COUNT, LG_OBJ_MAX_SIZE), generator);

        // small objects
        endToEndTest(TestObjectSource.generateRandomObjects(SM_OBJ_COUNT, SM_OBJ_MAX_SIZE), generator);
    }

    private <T extends SyncObject<T>> void endToEndTest(List<TestSyncObject> testObjects, PluginGenerator<T> generator) {
        try {
            TestObjectSource testSource = new TestObjectSource(testObjects);

            // send test data to test system
            ViPRSync sync = new ViPRSync();
            sync.setSource(testSource);
            sync.setTarget(generator.createTarget());
            sync.setSyncThreadCount(SYNC_THREAD_COUNT);
            sync.run();

            // read data from same system
            TestObjectTarget testTarget = new TestObjectTarget();
            sync = new ViPRSync();
            sync.setSource(generator.createSource());
            sync.setTarget(testTarget);
            sync.setSyncThreadCount(SYNC_THREAD_COUNT);
            sync.run();

            verifyObjects(testObjects, testTarget.getRootObjects(), "");
        } finally {
            try {
                // delete the objects from the test system
                SyncSource<T> source = generator.createSource();
                for (T object : source) {
                    recursiveDelete(source, object).get(); // wait for root to be deleted
                }
            } catch (Throwable t) {
                l4j.warn("could not delete objects after sync: " + t.getMessage());
            }
        }
    }

    private void verifyObjects(List<TestSyncObject> sourceObjects, List<TestSyncObject> targetObjects, String parentPath) {
        Assert.assertEquals(parentPath + " - object lists are different size", sourceObjects.size(), targetObjects.size());
        for (TestSyncObject sourceObject : sourceObjects) {
            for (TestSyncObject targetObject : targetObjects) {
                if (sourceObject.getRelativePath().equals(targetObject.getRelativePath())) {
                    String currentPath = sourceObject.getRelativePath();
                    Assert.assertEquals("relative paths not equal", sourceObject.getRelativePath(), targetObject.getRelativePath());
                    if (sourceObject.hasData()) {
                        Assert.assertTrue(currentPath + " - source has data but target does not", targetObject.hasData());
                        Assert.assertEquals(currentPath + " - data size different", sourceObject.getSize(), targetObject.getSize());
                        Assert.assertArrayEquals(currentPath + " - data not equal", sourceObject.getData(), targetObject.getData());
                    }
                    if (sourceObject.hasChildren()) {
                        Assert.assertTrue(currentPath + " - source has children but target does not", targetObject.hasChildren());
                        verifyObjects(sourceObject.getChildren(), targetObject.getChildren(), currentPath);
                    }
                }
            }
        }
    }

    private <T extends SyncObject<T>> Future recursiveDelete(final SyncSource<T> source, final T syncObject) throws ExecutionException, InterruptedException {
        List<Future> futures = new ArrayList<>();
        if (syncObject.hasChildren()) {
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
                l4j.warn(syncObject.getSourceIdentifier() + " deleted");
            }
        });
    }

    private interface PluginGenerator<T extends SyncObject<T>> {
        public SyncSource<T> createSource();

        public SyncTarget createTarget();
    }
}

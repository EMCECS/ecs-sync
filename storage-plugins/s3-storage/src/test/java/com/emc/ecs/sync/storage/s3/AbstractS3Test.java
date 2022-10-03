/*
 * Copyright (c) 2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.SkipObjectException;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.AwsS3Config;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.test.TestUtil;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.amazonaws.services.s3.model.BucketVersioningConfiguration.ENABLED;
import static com.amazonaws.services.s3.model.BucketVersioningConfiguration.SUSPENDED;
import static com.emc.ecs.sync.storage.s3.AbstractS3Storage.*;

public abstract class AbstractS3Test {
    private static final Logger log = LoggerFactory.getLogger(AbstractS3Test.class);

    protected static String getS3Endpoint() {
        return TestConfig.getPropertyNotEmpty(TestConfig.PROP_S3_ENDPOINT);
    }

    protected static String getStsEndpoint() {
        return TestConfig.getProperty(TestConfig.PROP_STS_ENDPOINT);
    }

    protected static String getIamEndpoint() {
        return TestConfig.getProperty(TestConfig.PROP_IAM_ENDPOINT);
    }

    protected static String getS3AccessKey() {
        return TestConfig.getPropertyNotEmpty(TestConfig.PROP_S3_ACCESS_KEY_ID);
    }

    protected static String getS3SecretKey() {
        return TestConfig.getPropertyNotEmpty(TestConfig.PROP_S3_SECRET_KEY);
    }

    protected static String getS3Region() {
        return TestConfig.getProperty(TestConfig.PROP_S3_REGION, "us-east-1");
    }

    public static AmazonS3 createS3Client() {
        // disable SSL validation in AWS SDK for testing (lab systems typically use self-signed certificates)
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

        ClientConfiguration config = new ClientConfiguration();
        String proxyUri = TestConfig.getProperty(TestConfig.PROP_HTTP_PROXY_URI);
        if (proxyUri != null) {
            URI uri = URI.create(proxyUri);
            config.setProxyHost(uri.getHost());
            config.setProxyPort(uri.getPort());
        }
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(getS3AccessKey(), getS3SecretKey())))
                .withClientConfiguration(config)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(getS3Endpoint(), getS3Region()));
        return builder.build();
    }

    protected abstract AbstractS3Storage<?> createStorageInstance();

    protected abstract Object generateConfig();

    protected abstract String getTestBucket();

    // for testing purposes (to interrogate objects), we just need any S3 client, so we'll use the AWS one
    protected AmazonS3 amazonS3;

    @BeforeEach
    public void initClientAndBucket() {
        amazonS3 = createS3Client();

        createBucket(getTestBucket(), false);
    }

    protected void createBucket(String bucket, boolean withVersioning) {
        createBucket(amazonS3, bucket, withVersioning);
    }

    public static void createBucket(AmazonS3 amazonS3, String bucket, boolean withVersioning) {
        try {
            amazonS3.createBucket(bucket);
        } catch (AmazonServiceException e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        if (withVersioning) {
            enableVersioning(amazonS3, bucket);
        }
    }

    protected void enableVersioning(String bucket) {
        enableVersioning(amazonS3, bucket);
    }

    public static void enableVersioning(AmazonS3 amazonS3, String bucket) {
        amazonS3.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucket,
                new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
    }

    @AfterEach
    public void destroyClientAndBucket() {
        deleteBucket(getTestBucket());
        if (amazonS3 != null) amazonS3.shutdown();
    }

    protected void deleteBucket(String bucket) {
        deleteBucket(amazonS3, bucket);
    }

    public static void deleteBucket(AmazonS3 amazonS3, String bucket) {
        if (amazonS3 != null) {
            try {
                // delete MPUs
                amazonS3.listMultipartUploads(new ListMultipartUploadsRequest(bucket)).getMultipartUploads().stream()
                        .parallel()
                        .forEach(upload -> amazonS3.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, upload.getKey(), upload.getUploadId())));
                if (Arrays.asList(SUSPENDED, ENABLED).contains(amazonS3.getBucketVersioningConfiguration(bucket).getStatus())) {
                    // delete versions
                    ListVersionsRequest request = new ListVersionsRequest().withBucketName(bucket);
                    VersionListing listing;
                    do {
                        listing = amazonS3.listVersions(request);
                        listing.getVersionSummaries().stream().parallel()
                                .forEach(version -> amazonS3.deleteVersion(bucket, version.getKey(), version.getVersionId()));
                        request.withKeyMarker(listing.getNextKeyMarker()).withVersionIdMarker(listing.getNextVersionIdMarker());
                    } while (listing.isTruncated());
                } else {
                    // delete objects
                    deleteObjectsInBucket(amazonS3, bucket);
                }
                amazonS3.deleteBucket(bucket);
            } catch (RuntimeException e) {
                log.warn("could not delete bucket " + bucket, e);
            }
        }
    }

    protected void deleteObjectsInBucket(String bucket) {
        deleteObjectsInBucket(amazonS3, bucket);
    }

    public static void deleteObjectsInBucket(AmazonS3 amazonS3, String bucket) {
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket);
        ObjectListing listing;
        do {
            listing = amazonS3.listObjects(request);
            listing.getObjectSummaries().stream().parallel()
                    .forEach(object -> amazonS3.deleteObject(bucket, object.getKey()));
            request.withMarker(listing.getNextMarker());
        } while (listing.isTruncated());
    }

    @Test
    public void testSkipIfExists() {
        Date newMtime = new Date();
        Date oldMtime = new Date(newMtime.getTime() - (10 * 3_600_000)); // 10 hours ago
        String normalEtag = "27dea2b9e19ee1533e1c6fa6288d9511";
        String mpuEtag = "b6715f06334ba04ee628ef1acc11ba25-150";
        String otherEtag = "3d56d1b48c2085c787457e7013beafb8";
        long normalSize = 1024, mpuSize = 2451212548L, otherSize = 10_000_000;

        // construct 2 test object contexts, 1 with MPU ETag, 1 with regular ETag
        TestStorage testStorage = new TestStorage();
        ObjectMetadata normalMetadata = new ObjectMetadata().withModificationTime(newMtime).withContentLength(normalSize).withHttpEtag(normalEtag);
        ObjectContext normalContext = new ObjectContext().withObject(new SyncObject(testStorage, "normal", normalMetadata));
        ObjectMetadata mpuMetadata = new ObjectMetadata().withModificationTime(newMtime).withContentLength(mpuSize).withHttpEtag(mpuEtag);
        ObjectContext mpuContext = new ObjectContext().withObject(new SyncObject(testStorage, "mpu", mpuMetadata));
        List<ObjectContext> contexts = Arrays.asList(normalContext, mpuContext);

        // this is only to have a reference imlementation
        try (AbstractS3Storage<?> storage = createStorageInstance()) {

            // for each object context, call skipIfExists(), passing in variations of targetObject
            for (ObjectContext context : contexts) {

                // 1. target has no source markers (x-emc-source-*), same size, newer mtime
                //    expect: SkipObjectException
                ObjectMetadata targetMetadata = new ObjectMetadata();
                targetMetadata.setContentLength(context.getObject().getMetadata().getContentLength());
                targetMetadata.setModificationTime(newMtime);
                try {
                    storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
                    Assertions.fail("legacy check with same size and newer mtime should throw SkipObjectException");
                } catch (SkipObjectException ignored) {
                }

                // 2. target has no source markers (x-emc-source-*), different size, newer mtime
                //    expect: no exception, no property flags
                targetMetadata = new ObjectMetadata();
                targetMetadata.setContentLength(otherSize);
                targetMetadata.setModificationTime(newMtime);
                storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
                Assertions.assertNull(context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));

                // 3. target has no source markers (x-emc-source-*), same size, older mtime
                //    expect: no exception, no property flags
                targetMetadata = new ObjectMetadata();
                targetMetadata.setContentLength(context.getObject().getMetadata().getContentLength());
                targetMetadata.setModificationTime(oldMtime);
                storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
                Assertions.assertNull(context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));

                // 4. target has no source markers (x-emc-source-*), different size, older mtime
                //    expect: no exception, no property flags
                targetMetadata = new ObjectMetadata();
                targetMetadata.setContentLength(otherSize);
                targetMetadata.setModificationTime(oldMtime);
                storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
                Assertions.assertNull(context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));

                // 5. target has source markers, different source-etag, older source-mtime
                //    expect: no exception, no property flags
                targetMetadata = new ObjectMetadata();
                targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_MTIME, String.valueOf(oldMtime.getTime()));
                targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_ETAG, otherEtag);
                storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
                Assertions.assertNull(context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));

                // 6. target has source markers, same source-etag, older source-mtime
                //    expect: no exception, sourceEtagMatches flag is true
                targetMetadata = new ObjectMetadata();
                targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_MTIME, String.valueOf(oldMtime.getTime()));
                targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_ETAG, context.getObject().getMetadata().getHttpEtag());
                storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
                Assertions.assertNotNull(context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));
                Assertions.assertTrue((Boolean) context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));
                // reset objectContext state
                context.getObject().removeProperty(PROP_SOURCE_ETAG_MATCHES);

                // 7. target has source markers, different source-etag, newer source-mtime
                //    expect: SkipObjectException
                targetMetadata = new ObjectMetadata();
                targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_MTIME, String.valueOf(newMtime.getTime()));
                targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_ETAG, otherEtag);
                try {
                    storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
                    Assertions.fail("newer source-mtime should throw SkipObjectException");
                } catch (SkipObjectException ignored) {
                }

                // 8. target has source markers, same source-etag, newer source-mtime
                //    expect: SkipObjectException
                targetMetadata = new ObjectMetadata();
                targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_MTIME, String.valueOf(newMtime.getTime()));
                targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_ETAG, context.getObject().getMetadata().getHttpEtag());
                try {
                    storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
                    Assertions.fail("newer source-mtime should throw SkipObjectException");
                } catch (SkipObjectException ignored) {
                }
            }
        }
    }

    @Test
    public void testSkipBehavior() throws Exception {
        // only enable this when testing against a local lab cluster
        Assumptions.assumeTrue(Boolean.parseBoolean(TestConfig.getProperties().getProperty(TestConfig.PROP_LARGE_DATA_TESTS)));

        int smObjectSize = 10_240; // 10K
        int lgObjectSize = 100 * 1024 * 1024; // 100M
        int objectCount = 100;

        com.emc.ecs.sync.config.storage.TestConfig testConfig = new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false);
        TestStorage testStorage = new TestStorage();
        testStorage.setConfig(testConfig);

        // create complete list of objects
        List<String> objectKeys = IntStream.range(0, objectCount).mapToObj(i -> "object" + i).collect(Collectors.toList());
        String[] sourceList = objectKeys.stream().map(k -> testStorage.getIdentifier(k, false)).toArray(String[]::new);

        // write half of objects to test storage
        // the first object will be large
        // must map the relative path to a test identifier first
        IntStream.range(0, objectCount / 2).forEach(i -> {
            int objSize = (i == 0) ? lgObjectSize : smObjectSize;
            ObjectMetadata metadata = new ObjectMetadata().withContentLength(objSize).withModificationTime(new Date());
            String testIdentifier = testStorage.getIdentifier(objectKeys.get(i), false);
            testStorage.updateObject(testIdentifier, testStorage.new TestSyncObject(testStorage, objectKeys.get(i), metadata));
        });

        // sync complete list
        Object targetConfig = generateConfig();
        if (targetConfig instanceof EcsS3Config) {
            EcsS3Config ecsConfig = (EcsS3Config) targetConfig;
            ecsConfig.setStoreSourceObjectCopyMarkers(true);
            ecsConfig.setMpuThresholdMb(lgObjectSize);
            ecsConfig.setMpuPartSizeMb(lgObjectSize / 8);
        } else {
            AwsS3Config awsConfig = (AwsS3Config) targetConfig;
            awsConfig.setStoreSourceObjectCopyMarkers(true);
            awsConfig.setMpuThresholdMb(lgObjectSize);
            awsConfig.setMpuPartSizeMb(lgObjectSize / 8);
        }

        SyncConfig syncConfig = new SyncConfig()
                .withTarget(targetConfig)
                .withOptions(new SyncOptions().withSourceList(sourceList));
        EcsSync syncJob = new EcsSync();
        syncJob.setSyncConfig(syncConfig);
        syncJob.setSource(testStorage);
        TestUtil.run(syncJob);

        // check success/skip/error count, byte count
        Assertions.assertEquals(objectCount / 2, syncJob.getStats().getObjectsComplete());
        Assertions.assertEquals(0, syncJob.getStats().getObjectsSkipped());
        Assertions.assertEquals(0, syncJob.getStats().getObjectsCopySkipped());
        Assertions.assertEquals(objectCount / 2, syncJob.getStats().getObjectsFailed());
        Assertions.assertEquals((objectCount / 2 - 1) * smObjectSize + lgObjectSize, syncJob.getStats().getBytesComplete());
        Assertions.assertEquals(0, syncJob.getStats().getBytesSkipped());
        Assertions.assertEquals(0, syncJob.getStats().getBytesCopySkipped());

        // store mtimes to make sure they don't change in the second copy
        List<Long> mtimes = IntStream.range(0, objectCount / 2).parallel().mapToObj(i ->
                amazonS3.getObjectMetadata(getTestBucket(), objectKeys.get(i)).getLastModified().getTime()
        ).collect(Collectors.toList());

        // wait a couple seconds to make sure new writes will get a different mtime
        Thread.sleep(2000);

        // write remaining objects to test storage
        IntStream.range(objectCount / 2, objectCount).forEach(i -> {
            int objSize = (i == objectCount / 2) ? lgObjectSize : smObjectSize;
            ObjectMetadata metadata = new ObjectMetadata().withContentLength(objSize).withModificationTime(new Date());
            String testIdentifier = testStorage.getIdentifier(objectKeys.get(i), false);
            testStorage.updateObject(testIdentifier, testStorage.new TestSyncObject(testStorage, objectKeys.get(i), metadata));
        });

        // sync complete list again
        syncJob = new EcsSync();
        syncJob.setSyncConfig(syncConfig);
        syncJob.setSource(testStorage);
        TestUtil.run(syncJob);

        // check success/skip/error count, byte count
        Assertions.assertEquals(objectCount / 2, syncJob.getStats().getObjectsComplete());
        Assertions.assertEquals(objectCount / 2, syncJob.getStats().getObjectsSkipped());
        Assertions.assertEquals(objectCount / 2, syncJob.getStats().getObjectsCopySkipped());
        Assertions.assertEquals(0, syncJob.getStats().getObjectsFailed());
        Assertions.assertEquals((objectCount / 2 - 1) * smObjectSize + lgObjectSize, syncJob.getStats().getBytesComplete());
        Assertions.assertEquals((objectCount / 2 - 1) * smObjectSize + lgObjectSize, syncJob.getStats().getBytesSkipped());
        Assertions.assertEquals((objectCount / 2 - 1) * smObjectSize + lgObjectSize, syncJob.getStats().getBytesCopySkipped());

        // check mtime of previously copied objects has not been changed
        IntStream.range(0, objectCount / 2).forEach(i ->
                Assertions.assertEquals(mtimes.get(i), amazonS3.getObjectMetadata(getTestBucket(), objectKeys.get(i)).getLastModified().getTime())
        );
    }

    @Test
    public void testForceSync() throws Exception {
        int objectSize = 10_240; // 10K
        int objectCount = 100;

        // test source
        com.emc.ecs.sync.config.storage.TestConfig testConfig = new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false);
        TestStorage testStorage = new TestStorage();
        testStorage.setConfig(testConfig);

        // create complete list of objects
        List<String> objectKeys = IntStream.range(0, objectCount).mapToObj(i -> "object" + i).collect(Collectors.toList());
        String[] sourceList = objectKeys.stream().map(k -> testStorage.getIdentifier(k, false)).toArray(String[]::new);

        // write objects to test storage
        // must map the relative path to a test identifier first
        // be sure to use an aged mtime, in case there is clock skew between test client and server
        Calendar oldMtime = Calendar.getInstance();
        oldMtime.add(Calendar.HOUR, -1); // 1 hour ago
        IntStream.range(0, objectCount).forEach(i -> {
            ObjectMetadata metadata = new ObjectMetadata().withContentLength(objectSize).withModificationTime(oldMtime.getTime());
            String testIdentifier = testStorage.getIdentifier(objectKeys.get(i), false);
            testStorage.updateObject(testIdentifier, testStorage.new TestSyncObject(testStorage, objectKeys.get(i), metadata));
        });

        // S3 target
        Object targetConfig = generateConfig();

        // copy all objects
        SyncConfig syncConfig = new SyncConfig()
                .withTarget(targetConfig)
                .withOptions(new SyncOptions().withSourceList(sourceList));
        EcsSync syncJob = new EcsSync();
        syncJob.setSyncConfig(syncConfig);
        syncJob.setSource(testStorage);
        TestUtil.run(syncJob);

        // check success/skip/error count, byte count
        Assertions.assertEquals(objectCount, syncJob.getStats().getObjectsComplete());
        Assertions.assertEquals(0, syncJob.getStats().getObjectsSkipped());
        Assertions.assertEquals(0, syncJob.getStats().getObjectsCopySkipped());
        Assertions.assertEquals(0, syncJob.getStats().getObjectsFailed());
        Assertions.assertEquals(objectCount * objectSize, syncJob.getStats().getBytesComplete());
        Assertions.assertEquals(0, syncJob.getStats().getBytesSkipped());
        Assertions.assertEquals(0, syncJob.getStats().getBytesCopySkipped());

        // store mtimes to make sure they don't change in the second copy
        List<Long> mtimes = IntStream.range(0, objectCount).parallel().mapToObj(i ->
                amazonS3.getObjectMetadata(getTestBucket(), testStorage.getRootObjects().get(i).getRelativePath()).getLastModified().getTime()
        ).collect(Collectors.toList());

        // wait a couple seconds to make sure new writes will get a different mtime
        Thread.sleep(2000);

        // run again (all objects should be skipped)
        syncJob = new EcsSync();
        syncJob.setSyncConfig(syncConfig);
        syncJob.setSource(testStorage);
        TestUtil.run(syncJob);

        // check success/skip/error count, byte count
        Assertions.assertEquals(0, syncJob.getStats().getObjectsComplete());
        Assertions.assertEquals(objectCount, syncJob.getStats().getObjectsSkipped());
        Assertions.assertEquals(objectCount, syncJob.getStats().getObjectsCopySkipped());
        Assertions.assertEquals(0, syncJob.getStats().getObjectsFailed());
        Assertions.assertEquals(0, syncJob.getStats().getBytesComplete());
        Assertions.assertEquals(objectCount * objectSize, syncJob.getStats().getBytesSkipped());
        Assertions.assertEquals(objectCount * objectSize, syncJob.getStats().getBytesCopySkipped());

        // check mtime and make sure *no* objects have changed
        IntStream.range(0, objectCount).forEach(i -> {
            long targetMtime = amazonS3.getObjectMetadata(getTestBucket(), testStorage.getRootObjects().get(i).getRelativePath()).getLastModified().getTime();
            Assertions.assertEquals(mtimes.get(i), targetMtime);
        });

        // now enable forceSync and run again
        syncConfig.getOptions().setForceSync(true);
        syncJob = new EcsSync();
        syncJob.setSyncConfig(syncConfig);
        syncJob.setSource(testStorage);
        TestUtil.run(syncJob);

        // check success/skip/error count, byte count
        Assertions.assertEquals(objectCount, syncJob.getStats().getObjectsComplete());
        Assertions.assertEquals(0, syncJob.getStats().getObjectsSkipped());
        Assertions.assertEquals(0, syncJob.getStats().getObjectsCopySkipped());
        Assertions.assertEquals(0, syncJob.getStats().getObjectsFailed());
        Assertions.assertEquals(objectCount * objectSize, syncJob.getStats().getBytesComplete());
        Assertions.assertEquals(0, syncJob.getStats().getBytesSkipped());
        Assertions.assertEquals(0, syncJob.getStats().getBytesCopySkipped());

        // check mtime and make sure *all* objects have changed
        IntStream.range(0, objectCount).forEach(i -> {
            long targetMtime = amazonS3.getObjectMetadata(getTestBucket(), testStorage.getRootObjects().get(i).getRelativePath()).getLastModified().getTime();
            Assertions.assertTrue(mtimes.get(i) < targetMtime);
        });
    }
}

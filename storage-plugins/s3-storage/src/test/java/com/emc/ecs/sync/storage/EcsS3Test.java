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

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import com.emc.ecs.sync.config.storage.S3ConfigurationException;
import com.emc.ecs.sync.model.Checksum;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.file.AbstractFilesystemStorage;
import com.emc.ecs.sync.storage.s3.EcsS3Storage;
import com.emc.ecs.sync.test.DelayFilter;
import com.emc.ecs.sync.test.StartNotifyFilter;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.util.EnhancedThreadPoolExecutor;
import com.emc.ecs.sync.util.RandomInputStream;
import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.*;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.AbortMultipartUploadRequest;
import com.emc.object.s3.request.ListMultipartUploadsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.util.ChecksumAlgorithm;
import com.emc.object.util.ChecksummedInputStream;
import com.emc.object.util.RestUtil;
import com.emc.object.util.RunningChecksum;
import com.emc.rest.util.StreamUtil;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EcsS3Test {
    private static final Logger log = LoggerFactory.getLogger(EcsS3Test.class);

    private final String bucketName = "ecs-sync-ecs-s3-target-test-bucket";
    private S3Client s3;
    private TestStorage testStorage;
    private EcsS3Storage storage;
    private String stsEndpoint;
    private AmazonIdentityManagement iamClient;
    private String region;
    private User alternateUser;

    private String altUserAccessKey;
    private String altUserSecretKey;

    @BeforeEach
    public void setup() throws Exception {
        Properties syncProperties = TestConfig.getProperties();
        String endpoint = syncProperties.getProperty(TestConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(TestConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(TestConfig.PROP_S3_SECRET_KEY);
        final boolean useVHost = Boolean.parseBoolean(syncProperties.getProperty(TestConfig.PROP_S3_VHOST));
        Assumptions.assumeTrue(endpoint != null && accessKey != null && secretKey != null);
        final URI endpointUri = new URI(endpoint);
        region = syncProperties.getProperty(TestConfig.PROP_S3_REGION, "us-east-1");

        S3Config s3Config;
        if (useVHost) s3Config = new S3Config(endpointUri);
        else s3Config = new S3Config(Protocol.valueOf(endpointUri.getScheme().toUpperCase()), endpointUri.getHost());
        s3Config.withPort(endpointUri.getPort()).withUseVHost(useVHost).withIdentity(accessKey).withSecretKey(secretKey);

        s3 = new S3JerseyClient(s3Config);

        try {
            s3.createBucket(bucketName);
        } catch (S3Exception e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        testStorage = new TestStorage();
        testStorage.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).withOptions(new SyncOptions());

        EcsS3Config config = new EcsS3Config();
        config.setProtocol(com.emc.ecs.sync.config.Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
        config.setHost(endpointUri.getHost());
        config.setPort(endpointUri.getPort());
        config.setEnableVHosts(useVHost);
        config.setAccessKey(accessKey);
        config.setSecretKey(secretKey);
        config.setBucketName(bucketName);
        // make sure we don't hang forever on a stuck read
        config.setSocketReadTimeoutMs(30_000);

        storage = new EcsS3Storage();
        storage.setConfig(config);
        storage.setOptions(new SyncOptions());
        storage.configure(testStorage, null, storage);

        this.stsEndpoint = syncProperties.getProperty(TestConfig.PROP_STS_ENDPOINT);
        String iamEndpoint = syncProperties.getProperty(TestConfig.PROP_IAM_ENDPOINT);

        // disable SSL validation in AWS SDK for testing (lab systems typically use self-signed certificates)
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

        String username = "ecs-sync-s3-test-user";
        if (iamEndpoint != null) {
            // create alternate IAM user for access tests
            this.iamClient =
                    AmazonIdentityManagementClientBuilder.standard()
                            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(iamEndpoint, region))
                            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                                    s3Config.getIdentity(), s3Config.getSecretKey()))).build();
            try {
                this.alternateUser = iamClient.createUser(new CreateUserRequest(username)).getUser();
            } catch (EntityAlreadyExistsException e) {
                this.alternateUser = iamClient.getUser(new GetUserRequest().withUserName(username)).getUser();
            }
            AccessKey tempAccessKey = iamClient.createAccessKey(new CreateAccessKeyRequest(alternateUser.getUserName())).getAccessKey();
            this.altUserAccessKey = tempAccessKey.getAccessKeyId();
            this.altUserSecretKey = tempAccessKey.getSecretAccessKey();
        }
    }

    @AfterEach
    public void teardown() {
        if (testStorage != null) testStorage.close();
        if (storage != null) storage.close();
        ListMultipartUploadsResult result = s3.listMultipartUploads(new ListMultipartUploadsRequest(bucketName));
        for (Upload upload : result.getUploads()) {
            s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, upload.getKey(), upload.getUploadId()));
        }
        deleteBucket(s3, bucketName);
        s3.destroy();
        if (alternateUser != null) {
            // must delete access keys first
            for (AccessKeyMetadata accessKeyMeta : iamClient.listAccessKeys(new ListAccessKeysRequest().withUserName(alternateUser.getUserName())).getAccessKeyMetadata()) {
                iamClient.deleteAccessKey(new DeleteAccessKeyRequest(alternateUser.getUserName(), accessKeyMeta.getAccessKeyId()));
            }
            iamClient.deleteUser(new DeleteUserRequest(alternateUser.getUserName()));
        }
    }

    @Test
    public void testNormalUpload() throws Exception {
        String key = "normal-upload";
        long size = 512 * 1024; // 512KiB
        InputStream stream = new RandomInputStream(size);
        SyncObject object = new SyncObject(storage, key, new ObjectMetadata().withContentLength(size), stream, null);

        storage.updateObject(key, object);

        // proper ETag means no MPU was performed
        Assertions.assertEquals(object.getMd5Hex(true).toUpperCase(), s3.getObjectMetadata(bucketName, key).getETag().toUpperCase());
    }

    @Test
    public void testCifsEcs() throws Exception {
        String sBucket = "ecs-sync-cifs-ecs-test";
        String key = "empty-file";
        s3.createBucket(sBucket);
        s3.putObject(sBucket, key, (Object) null, null);

        try {
            EcsS3Config source = new EcsS3Config();
            source.setProtocol(storage.getConfig().getProtocol());
            source.setHost(storage.getConfig().getHost());
            source.setPort(storage.getConfig().getPort());
            source.setEnableVHosts(storage.getConfig().isEnableVHosts());
            source.setAccessKey(storage.getConfig().getAccessKey());
            source.setSecretKey(storage.getConfig().getSecretKey());
            source.setBucketName(sBucket);
            source.setApacheClientEnabled(true);

            SyncConfig config = new SyncConfig().withSource(source);
            config.getOptions().setRetryAttempts(0); // disable retries for brevity

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(config);
            sync.setTarget(storage);
            sync.run();

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
            Assertions.assertNotNull(s3.getObjectMetadata(bucketName, key));
        } finally {
            s3.deleteObject(sBucket, key);
            s3.deleteBucket(sBucket);
        }
    }

    @Test
    public void testCrazyCharacters() throws Exception {
        String[] keys = {
                "foo!@#$%^&*()-_=+",
                "bar\u00a1\u00bfbar",
                "查找的unicode",
                "baz\u0007bim",
                "bim\u0006-boozle"
        };

        String sBucket = "ecs-sync-crazy-char-test";

        s3.createBucket(sBucket);
        for (String key : keys) {
            s3.putObject(sBucket, key, (Object) null, null);
        }

        try {
            EcsS3Config s3Config = new EcsS3Config();
            s3Config.setProtocol(storage.getConfig().getProtocol());
            s3Config.setHost(storage.getConfig().getHost());
            s3Config.setPort(storage.getConfig().getPort());
            s3Config.setEnableVHosts(storage.getConfig().isEnableVHosts());
            s3Config.setAccessKey(storage.getConfig().getAccessKey());
            s3Config.setSecretKey(storage.getConfig().getSecretKey());
            s3Config.setBucketName(sBucket);
            s3Config.setApacheClientEnabled(true);
            s3Config.setUrlEncodeKeys(true);

            SyncConfig config = new SyncConfig().withSource(s3Config).withTarget(new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false));
            config.getOptions().withVerify(true).setRetryAttempts(0); // disable retries for brevity

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(config);
            sync.run();

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(keys.length, sync.getStats().getObjectsComplete());
        } finally {
            for (String key : keys) {
                s3.deleteObject(sBucket, key);
            }
            s3.deleteBucket(sBucket);
        }
    }

    @Test
    public void testVeryLargeUploadStream() throws Exception {
        // only enable this when testing against a local lab cluster
        Assumptions.assumeTrue(Boolean.parseBoolean(TestConfig.getProperties().getProperty(TestConfig.PROP_LARGE_DATA_TESTS)));

        String key = "large-stream-upload";
        long size = 128L * 1024 * 1024 + 10; // 128MB + 10 bytes
        storage.getConfig().setMpuEnabled(true);
        storage.getConfig().setMpuThresholdMb(128);
        storage.getConfig().setMpuPartSizeMb(8);
        InputStream stream = new RandomInputStream(size);

        SyncObject object = new SyncObject(testStorage, key, new ObjectMetadata().withContentLength(size), stream, null);

        storage.updateObject(key, object);

        // hyphen denotes an MPU
        Assertions.assertTrue(s3.getObjectMetadata(bucketName, key).getETag().contains("-"));

        // verify bytes read from source
        // first wait a tick so the perf counter has at least one interval
        Thread.sleep(1000);
        Assertions.assertEquals(size, object.getBytesRead());
        Assertions.assertTrue(testStorage.getReadRate() > 0);

        // need to read the entire object since we can't use the ETag
        InputStream objectStream = s3.getObject(bucketName, key).getObject();
        ChecksummedInputStream md5Stream = new ChecksummedInputStream(objectStream, new RunningChecksum(ChecksumAlgorithm.MD5));
        byte[] buffer = new byte[128 * 1024];
        int c;
        do {
            c = md5Stream.read(buffer);
        } while (c >= 0);
        md5Stream.close();

        Assertions.assertEquals(object.getMd5Hex(true).toUpperCase(), md5Stream.getChecksum().getHexValue().toUpperCase());
    }

    @Test
    public void testVeryLargeUploadFile() throws Exception {
        // only enable this when testing against a local lab cluster
        Assumptions.assumeTrue(Boolean.parseBoolean(TestConfig.getProperties().getProperty(TestConfig.PROP_LARGE_DATA_TESTS)));

        String key = "large-file-upload";
        long size = 128L * 1024 * 1024 + 10; // 128MB + 10 bytes
        storage.getConfig().setMpuEnabled(true);
        storage.getConfig().setMpuThresholdMb(128);
        storage.getConfig().setMpuPartSizeMb(8);
        InputStream stream = new RandomInputStream(size);

        // create temp file
        File tempFile = File.createTempFile(key, null);
        tempFile.deleteOnExit();
        StreamUtil.copy(stream, new FileOutputStream(tempFile), size);

        SyncObject object = new SyncObject(testStorage, key, new ObjectMetadata().withContentLength(size), new FileInputStream(tempFile), null);
        object.setProperty(AbstractFilesystemStorage.PROP_FILE, tempFile);

        storage.updateObject(key, object);

        // hyphen denotes an MPU
        Assertions.assertTrue(s3.getObjectMetadata(bucketName, key).getETag().contains("-"));

        // verify bytes read from source
        // first wait a tick so the perf counter has at least one interval
        Thread.sleep(1000);
        Assertions.assertEquals(size, object.getBytesRead());
        Assertions.assertTrue(testStorage.getReadRate() > 0);

        // need to read the entire object since we can't use the ETag
        InputStream objectStream = s3.getObject(bucketName, key).getObject();
        ChecksummedInputStream md5Stream = new ChecksummedInputStream(objectStream, new RunningChecksum(ChecksumAlgorithm.MD5));
        byte[] buffer = new byte[128 * 1024];
        int c;
        do {
            c = md5Stream.read(buffer);
        } while (c >= 0);
        md5Stream.close();

        Assertions.assertEquals(object.getMd5Hex(true).toUpperCase(), md5Stream.getChecksum().getHexValue().toUpperCase());
    }

    @Test
    public void testInvalidContentType() {
        String key = "invalid-content-type";

        SyncObject object = new SyncObject(testStorage, key,
                new ObjectMetadata().withContentLength(0).withContentType("foo"), new ByteArrayInputStream(new byte[0]),
                null);

        storage.updateObject(key, object);

        Assertions.assertEquals(RestUtil.DEFAULT_CONTENT_TYPE, s3.getObjectMetadata(bucketName, key).getContentType());
    }

    // TODO: find a way to test this with ECS
    //       ECS only implements the AssumeRole API, so maybe we create a role, attach it to the alternate user, and
    //       assume that role here - then clean everything up in teardown()
    @Test
    public void testTemporaryCredentials() {
        // must have an STS endpoint
        Assumptions.assumeTrue(stsEndpoint != null);

        EcsS3Config s3Config = storage.getConfig();
        com.emc.ecs.sync.config.storage.TestConfig testConfig = testStorage.getConfig();
        testConfig.setDiscardData(false); // make sure we keep track of objects

        // use AWS SDK to get session token (until this is supported in ECS SDK)
        AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(stsEndpoint, region))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        s3Config.getAccessKey(), s3Config.getSecretKey()))).build();
        Credentials sessionCreds = sts.getSessionToken().getCredentials();

        // set credentials on storage config
        s3Config.setAccessKey(sessionCreds.getAccessKeyId());
        s3Config.setSecretKey(sessionCreds.getSecretAccessKey());
        s3Config.setSessionToken(sessionCreds.getSessionToken());

        // test as target

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withTarget(s3Config));
        sync.setSource(testStorage);
        sync.run();

        int totalGeneratedObjects = testStorage.getTotalObjectCount();

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(totalGeneratedObjects, sync.getStats().getObjectsComplete());

        // test as source

        testStorage.close();
        testStorage = new TestStorage();
        testStorage.withConfig(testConfig).withOptions(new SyncOptions());

        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withSource(s3Config));
        sync.setTarget(testStorage);
        sync.run();

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(totalGeneratedObjects, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(totalGeneratedObjects, testStorage.getTotalObjectCount());
    }

    public static void deleteBucket(final S3Client s3, final String bucket) {
        try {
            EnhancedThreadPoolExecutor executor = new EnhancedThreadPoolExecutor(30,
                    new LinkedBlockingDeque<Runnable>(2100), "object-deleter");

            ListObjectsResult listing = null;
            do {
                if (listing == null) listing = s3.listObjects(bucket);
                else listing = s3.listMoreObjects(listing);

                for (final S3Object summary : listing.getObjects()) {
                    executor.blockingSubmit(new Runnable() {
                        @Override
                        public void run() {
                            s3.deleteObject(bucket, summary.getKey());
                        }
                    });
                }
            } while (listing.isTruncated());

            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.MINUTES);

            s3.deleteBucket(bucket);
        } catch (RuntimeException e) {
            log.error("could not delete bucket " + bucket, e);
        } catch (InterruptedException e) {
            log.error("timed out while waiting for objects to be deleted");
        }
    }

    private boolean isRetentionInRange(long expectedValue, long retentionPeriod) {
        return Math.abs(expectedValue - retentionPeriod) <= 2;
    }

    @Test
    public void testUploadWithRetention() throws Exception {
        try {
            testStorage = new TestStorage();
            testStorage.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).
                    withOptions(new SyncOptions().withSyncRetentionExpiration(true));

            EcsS3Storage retentionStorage = new EcsS3Storage();
            retentionStorage.setConfig(storage.getConfig());
            retentionStorage.setOptions(new SyncOptions().withSyncRetentionExpiration(true));
            retentionStorage.configure(testStorage, null, retentionStorage);

            String key = "retention-upload";
            long size = 512 * 1024; // 512KiB
            InputStream stream = new RandomInputStream(size);

            long currentDate = System.currentTimeMillis();
            Date retentionTime = new Date(currentDate + 10 * 1000); // 10 sec
            SyncObject object = new SyncObject(retentionStorage, key,
                    new ObjectMetadata().withContentLength(size).withRetentionEndDate(retentionTime), stream, null);
            retentionStorage.updateObject(key, object);
            long retentionPeriod = TimeUnit.MILLISECONDS.toSeconds(retentionTime.getTime() - currentDate);
            Assertions.assertTrue(isRetentionInRange(retentionPeriod, s3.getObjectMetadata(bucketName, key).getRetentionPeriod()));
        } finally {
            // in case any assertions fail or there is any other error, we need to wait for retention to expire before trying to delete the object
            Thread.sleep(15 * 1000);
        }
    }

    @Test
    public void testSyncMetaFromS3MetaWithRetention() throws Exception {
        EcsS3Storage retentionStorage = new EcsS3Storage();
        retentionStorage.setConfig(storage.getConfig());
        retentionStorage.setOptions(new SyncOptions().withSyncRetentionExpiration(true));

        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata();
        s3ObjectMetadata.withContentType("")
                .withContentLength(0l)
                .withRetentionPeriod(100l)  // 100 secs
                .withCacheControl("")
                .withContentDisposition("")
                .withContentEncoding("")
                .withHttpExpires(new Date());
        s3ObjectMetadata.setExpirationDate(new Date());
        // set mtime to 10 seconds ago - this leaves 90 seconds left of retention
        s3ObjectMetadata.setLastModified(new Date(System.currentTimeMillis() - 10000));
        s3ObjectMetadata.setUserMetadata(new HashMap<>());

        Method privateSyncMetaFromS3Meta = retentionStorage.getClass().getDeclaredMethod("syncMetaFromS3Meta", S3ObjectMetadata.class);
        privateSyncMetaFromS3Meta.setAccessible(true);
        ObjectMetadata objectMetadata = (ObjectMetadata) privateSyncMetaFromS3Meta.invoke(retentionStorage, s3ObjectMetadata);
        Assertions.assertTrue(isRetentionInRange(90l, TimeUnit.MILLISECONDS.toSeconds(objectMetadata.getRetentionEndDate().getTime() - System.currentTimeMillis())));
    }

    @Test
    public void testSendContentMd5() {
        String goodKey = "valid-content-md5";
        String badKey = "invalid-content-md5";
        String data = "Dummy data to generate an MD5.";

        String goodMd5Hex = "d731e47a8731774b9b88ba89dbb1c7ec";
        String badMd5Hex = "e731e47a8731774b9b88ba89dbb1c7ec";

        SyncObject goodObject = new SyncObject(testStorage, goodKey,
                new ObjectMetadata().withContentLength(data.length()).withContentType("text/plain").withChecksum(Checksum.fromHex("MD5", goodMd5Hex)),
                new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)),
                null);
        SyncObject badObject = new SyncObject(testStorage, badKey,
                new ObjectMetadata().withContentLength(data.length()).withContentType("text/plain").withChecksum(Checksum.fromHex("MD5", badMd5Hex)),
                new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)),
                null);

        // this should work
        storage.updateObject(goodKey, goodObject);
        try {
            // but this should fail
            storage.updateObject(badKey, badObject);
            Assertions.fail("sending bad checksum should fail the write");
        } catch (RuntimeException e) {
            Assertions.assertTrue(e.getCause() instanceof S3Exception);
            Assertions.assertEquals(400, ((S3Exception) e.getCause()).getHttpCode());
            Assertions.assertTrue(((S3Exception) e.getCause()).getMessage().contains("Content-MD5"));
        }

        // make sure bad object does not exist
        try {
            s3.getObjectMetadata(bucketName, badKey);
        } catch (S3Exception e) {
            Assertions.assertEquals(404, e.getHttpCode());
        }
    }

    @Test
    public void testSkipExistingSameTarget() {
        // the test is for non-MPU objects only
        String key = "ecs-sync-test-existing-object";
        String data = "Dummy data to generate an MD5.";
        PutObjectRequest request = new PutObjectRequest(bucketName, key, data)
                .withObjectMetadata(new S3ObjectMetadata().withContentLength(data.length()).withContentType("text/plain"));
        PutObjectResult result = s3.putObject(request);
        Date lastModified = s3.getObjectMetadata(bucketName, key).getLastModified();
        String eTag = s3.getObjectMetadata(bucketName, key).getETag();

        SyncConfig syncConfig = new SyncConfig().withTarget(storage.getConfig()).withSource(testStorage.getConfig().withDiscardData(false));
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        sync.setTarget(storage);

        testStorage.createObject(new SyncObject(testStorage, key,
                new ObjectMetadata().withContentLength(data.length()).withContentType("text/plain").withHttpEtag(eTag),
                new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), null));

        storage.getOptions().setSyncMetadata(false);
        sync.run();
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
        // target should not get modified because Etag for If-None-Match PUT is of the same
        Assertions.assertEquals(lastModified, s3.getObjectMetadata(bucketName, key).getLastModified());

        storage.getOptions().setSyncMetadata(true);
        sync = new EcsSync(); // cannot reuse an instance
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        sync.setTarget(storage);
        sync.run();

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
        // Metadata gets updated while data writing is skipped(verified in previous test)
        Assertions.assertTrue(lastModified.before(s3.getObjectMetadata(bucketName, key).getLastModified()));

        lastModified = s3.getObjectMetadata(bucketName, key).getLastModified();
        storage.getOptions().setSyncMetadata(false);
        storage.getOptions().setForceSync(true);
        sync = new EcsSync(); // cannot reuse an instance
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        sync.setTarget(storage);
        sync.run();

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
        // force Sync should overwrite existing object
        Assertions.assertTrue(lastModified.before(s3.getObjectMetadata(bucketName, key).getLastModified()));
    }

    @Test
    public void testSafeDeletion() throws Exception {
        String ecsVersion = s3.listDataNodes().getVersionInfo();
        Assumptions.assumeTrue(ecsVersion != null && ecsVersion.compareTo("3.7.1") >= 0, "ECS version must be at least 3.7.1 or above");

        String key = "safe-deletion-test";
        s3.putObject(bucketName, key, "Original Content", "text/plain");
        SyncObject object = storage.loadObject(key);
        Assertions.assertNotNull(object.getMetadata().getHttpEtag());
        Assertions.assertEquals(object.getMetadata().getHttpEtag(), s3.getObjectMetadata(bucketName, key).getETag());

        s3.putObject(bucketName, key, "Modified Content", "text/plain");
        Assertions.assertNotEquals(object.getMetadata().getHttpEtag(), s3.getObjectMetadata(bucketName, key).getETag());

        // object shouldn't get deleted because Etag for If-Match is out-dated.
        storage.delete(key, object);
        try {
            object = storage.loadObject(key);
        } catch (ObjectNotFoundException e) {
            Assertions.fail("Source Object has been modified during sync, deletion is expected to fail.");
        }

        Assertions.assertNotNull(object.getMetadata().getHttpEtag());
        Assertions.assertEquals(object.getMetadata().getHttpEtag(), s3.getObjectMetadata(bucketName, key).getETag());
        // object should get deleted because Etag for If-Match has been refreshed by loadObject
        storage.delete(key, object);
        Assertions.assertThrows(ObjectNotFoundException.class, () -> {
            storage.loadObject(key);
        });
    }

    @Test
    public void testBucketWriteAccess() {
        Assumptions.assumeTrue(altUserSecretKey != null);
        String origAccessKey = storage.getConfig().getAccessKey();
        String origSecretKey = storage.getConfig().getSecretKey();
        try {
            s3.setBucketPolicy(bucketName, getAltUserReadPolicy());
            storage.getConfig().setAccessKey(altUserAccessKey);
            storage.getConfig().setSecretKey(altUserSecretKey);
            storage.configure(null, null, storage);
            Assertions.fail("expected 403");
        } catch (S3ConfigurationException e) {
            Assertions.assertEquals(S3ConfigurationException.Error.ERROR_BUCKET_ACCESS_WRITE, e.getError());
        } finally {
            s3.deleteBucketPolicy(bucketName);
        }

        storage.getConfig().setAccessKey(origAccessKey);
        storage.getConfig().setSecretKey(origSecretKey);
        storage.configure(null, null, storage);

        // verify no object was written to the bucket
        Assertions.assertEquals(0, s3.listObjects(bucketName).getObjects().size());
    }

    @Test
    public void testMpuTerminateResume() throws InterruptedException, ExecutionException {
        testMpuTerminateResume(true);
    }

    @Test
    public void testMpuTerminateNoResume() throws InterruptedException, ExecutionException {
        testMpuTerminateResume(false);
    }

    private void testMpuTerminateResume(boolean enableResume) throws ExecutionException, InterruptedException {
        String key = enableResume ? "testMpuTerminateEnableResume" : "testMpuTerminateDisableResume";
        int delayMs = 5000; // storage plugin only checks for termination every 5 seconds
        AwsS3LargeFileUploaderTest.MockMultipartSource mockMultipartSource = new AwsS3LargeFileUploaderTest.MockMultipartSource();

        EcsS3Config targetConfig = storage.getConfig();
        targetConfig.setMpuEnabled(true);
        targetConfig.setMpuResumeEnabled(enableResume);
        targetConfig.setMpuPartSizeMb((int) mockMultipartSource.getPartSize() / 1024 / 1024);
        targetConfig.setMpuThresholdMb(targetConfig.getMpuPartSizeMb());

        StartNotifyFilter.StartNotifyConfig startNotifyConfig = new StartNotifyFilter.StartNotifyConfig();

        SyncConfig syncConfig = new SyncConfig()
                .withTarget(targetConfig)
                .withSource(testStorage.getConfig())
                .withOptions(new SyncOptions().withThreadCount(2));
        // this will delay the first read of the data stream
        syncConfig.withFilters(Arrays.asList(startNotifyConfig, new DelayFilter.DelayConfig().withDataStreamDelayMs(delayMs)));

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        sync.setTarget(storage);

        // ingest the object into the source first
        testStorage.getConfig().withDiscardData(false);
        testStorage.createObject(new SyncObject(testStorage, key,
                new ObjectMetadata().withContentLength(mockMultipartSource.getTotalSize())
                        .withContentType("text/plain"),
                mockMultipartSource.getCompleteDataStream(), null));

        CompletableFuture<?> future = CompletableFuture.runAsync(sync);
        startNotifyConfig.waitForStart();
        Thread.sleep(delayMs);
        sync.terminate();
        future.get();

        if (enableResume) {
            // object should not exist
            Assertions.assertThrows(S3Exception.class, () -> s3.getObjectMetadata(bucketName, key));
            // MPU should exist
            ListMultipartUploadsResult result = s3.listMultipartUploads(new ListMultipartUploadsRequest(bucketName).withPrefix(key));
            Assertions.assertEquals(1, result.getUploads().size());
            // MPU should have at least 1 part
            String uploadId = result.getUploads().get(0).getUploadId();
            Assertions.assertTrue(s3.listParts(bucketName, key, uploadId).getParts().size() >= 1);

            EcsSync sync2 = new EcsSync();
            sync2.setSyncConfig(syncConfig);
            sync2.setSource(testStorage);
            sync2.setTarget(storage);

            sync2.run();
            Assertions.assertEquals(1, sync2.getStats().getObjectsComplete());
        } else {
            Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
        }

        Assertions.assertEquals(0, s3.listMultipartUploads(new ListMultipartUploadsRequest(bucketName).withPrefix(key)).getUploads().size());
        S3ObjectMetadata metadata = s3.getObjectMetadata(bucketName, key);
        Assertions.assertEquals(mockMultipartSource.getMpuETag(), metadata.getETag());
        Assertions.assertEquals(mockMultipartSource.getTotalSize(), metadata.getContentLength());
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
        EcsS3Config targetConfig = storage.getConfig();
        targetConfig.setStoreSourceObjectCopyMarkers(true);
        targetConfig.setMpuThresholdMb(lgObjectSize);
        targetConfig.setMpuPartSizeMb(lgObjectSize / 8);

        SyncConfig syncConfig = new SyncConfig()
                .withTarget(targetConfig)
                .withOptions(new SyncOptions().withSourceList(sourceList));
        EcsSync syncJob = new EcsSync();
        syncJob.setSyncConfig(syncConfig);
        syncJob.setSource(testStorage);
        syncJob.run();

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
                s3.getObjectMetadata(bucketName, objectKeys.get(i)).getLastModified().getTime()
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
        syncJob.run();

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
                Assertions.assertEquals(mtimes.get(i), s3.getObjectMetadata(bucketName, objectKeys.get(i)).getLastModified().getTime())
        );
    }

    private BucketPolicy getAltUserReadPolicy() {
        return new BucketPolicy()
                .withVersion("2012-10-17")
                .withId("ecs-sync-test-read-policy")
                .withStatements(Arrays.asList(
                        new BucketPolicyStatement()
                                .withSid("alt-user-get-object")
                                .withPrincipal("{\"AWS\":\"" + alternateUser.getArn() + "\"}")
                                .withEffect(BucketPolicyStatement.Effect.Allow)
                                .withActions(BucketPolicyAction.GetObject)
                                .withResource("arn:aws:s3:::" + bucketName + "/*"),
                        new BucketPolicyStatement()
                                .withSid("alt-user-head-bucket")
                                .withPrincipal("{\"AWS\":\"" + alternateUser.getArn() + "\"}")
                                .withEffect(BucketPolicyStatement.Effect.Allow)
                                .withActions(BucketPolicyAction.ListBucket)
                                .withResource("arn:aws:s3:::" + bucketName)
                ));
    }

    @Test
    public void testBandwidthThrottle() {
        int bandwidthLimit = 2 * 1024 * 1024; // throttle at 2 MiB/s
        int objectCount = 10, objectSize = 1024 * 1024; // 10 objects of 1 MiB size
        double DELTA_PERCENTAGE = 0.1; // allowed deviation

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig()
                .withSource(new com.emc.ecs.sync.config.storage.TestConfig()
                        .withChanceOfChildren(0).withObjectCount(objectCount)
                        .withMinSize(objectSize).withMaxSize(objectSize))
                .withTarget(new com.emc.ecs.sync.config.storage.TestConfig())
                .withOptions(new SyncOptions().withBandwidthLimit(bandwidthLimit)));
        sync.run();
        log.warn("expected bandwidth rate: {}, actual rate: {}", bandwidthLimit, sync.getSource().getReadRate());

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(objectCount, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(bandwidthLimit, sync.getSource().getReadRate(), bandwidthLimit * DELTA_PERCENTAGE);
    }

    @Test
    public void testTpsThrottle() {
        int tpsLimit = 20; //TPS throttle at objects/s
        double DELTA_PERCENTAGE = 0.1; //allowed deviation
        double DELTA_TPS = tpsLimit * DELTA_PERCENTAGE < 1 ? 1 : tpsLimit * DELTA_PERCENTAGE;
        testStorage = new TestStorage();
        testStorage.withConfig(new com.emc.ecs.sync.config.storage.TestConfig().withMaxSize(0));
        EcsS3Config s3Config = storage.getConfig();

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withTarget(s3Config).withOptions(new SyncOptions().withThroughputLimit(tpsLimit)));
        sync.setSource(testStorage);
        sync.run();

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(tpsLimit, sync.getStats().getObjectCompleteRate(), DELTA_TPS);
    }
}

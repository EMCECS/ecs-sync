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
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.RetentionMode;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.EcsRetentionType;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import com.emc.ecs.sync.config.storage.S3ConfigurationException;
import com.emc.ecs.sync.model.Checksum;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.file.AbstractFilesystemStorage;
import com.emc.ecs.sync.storage.s3.AbstractS3Test;
import com.emc.ecs.sync.storage.s3.EcsS3Storage;
import com.emc.ecs.sync.test.DelayFilter;
import com.emc.ecs.sync.test.StartNotifyFilter;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.test.TestUtil;
import com.emc.ecs.sync.util.RandomInputStream;
import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.*;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.*;
import com.emc.object.util.ChecksumAlgorithm;
import com.emc.object.util.ChecksummedInputStream;
import com.emc.object.util.RestUtil;
import com.emc.object.util.RunningChecksum;
import com.emc.rest.util.StreamUtil;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class EcsS3Test extends AbstractS3Test {
    private static final Logger log = LoggerFactory.getLogger(EcsS3Test.class);

    private S3Client ecsS3;
    private TestStorage testStorage;
    private EcsS3Storage storage;
    private AmazonIdentityManagement iamClient;
    private User alternateUser;

    private String altUserAccessKey;
    private String altUserSecretKey;

    private Role iamRole;

    @Override
    protected EcsS3Storage createStorageInstance() {
        return (EcsS3Storage) new EcsS3Storage().withConfig(generateConfig());
    }

    @Override
    protected EcsS3Config generateConfig() {
        return generateConfig(getTestBucket());
    }

    protected EcsS3Config generateConfig(String bucket) {
        URI endpointUri = URI.create(getS3Endpoint());
        boolean useVHost = Boolean.parseBoolean(TestConfig.getProperty(TestConfig.PROP_S3_VHOST));

        EcsS3Config config = new EcsS3Config();
        config.setProtocol(com.emc.ecs.sync.config.Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
        config.setHost(endpointUri.getHost());
        config.setPort(endpointUri.getPort());
        config.setEnableVHosts(useVHost);
        config.setAccessKey(getS3AccessKey());
        config.setSecretKey(getS3SecretKey());
        config.setBucketName(bucket);
        // make sure we don't hang forever on a stuck read
        config.setSocketReadTimeoutMs(30_000);
        return config;
    }

    @Override
    protected String getTestBucket() {
        return "ecs-sync-ecs-s3-test-bucket";
    }

    @BeforeEach
    public void setup() {
        final URI endpointUri = URI.create(getS3Endpoint());
        boolean useVHost = Boolean.parseBoolean(TestConfig.getProperty(TestConfig.PROP_S3_VHOST));

        S3Config s3Config;
        if (useVHost) s3Config = new S3Config(endpointUri);
        else s3Config = new S3Config(Protocol.valueOf(endpointUri.getScheme().toUpperCase()), endpointUri.getHost());
        s3Config.withPort(endpointUri.getPort()).withUseVHost(useVHost).withIdentity(getS3AccessKey()).withSecretKey(getS3SecretKey());

        ecsS3 = new S3JerseyClient(s3Config);

        try {
            ecsS3.createBucket(getTestBucket());
        } catch (S3Exception e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        testStorage = new TestStorage();
        testStorage.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).withOptions(new SyncOptions());

        storage = createStorageInstance();
        storage.setOptions(new SyncOptions());
        storage.configure(testStorage, null, storage);

        // disable SSL validation in AWS SDK for testing (lab systems typically use self-signed certificates)
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

        String username = "ecs-sync-s3-test-user";
	String roleName = "ecs-sync-s3-test-user-role";
        if (getIamEndpoint() != null) {
            // create alternate IAM user for access tests
            this.iamClient =
                    AmazonIdentityManagementClientBuilder.standard()
                            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(getIamEndpoint(), getS3Region()))
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

            String POLICY_DOCUMENT = "{ \"Version\": \"2012-10-17\",\n" +
                    "  \"Statement\": [\n" +
                    "    {\n" +
                    "      \"Action\": \"sts:AssumeRole\"," +
                    "      \"Resource\": \"*\",\n" +
                    "      \"Principal\": { \"AWS\": \"" + this.alternateUser.getArn().split(":user/")[0] + ":root\" },\n" +
                    "      \"Effect\": \"Allow\"\n" +
                    "    }\n" +
                    "  ]\n }";
            try {
                CreateRoleResult result2 = iamClient.createRole(new CreateRoleRequest().withRoleName(roleName).withAssumeRolePolicyDocument(POLICY_DOCUMENT));
                iamRole = result2.getRole();
            } catch (EntityAlreadyExistsException e) {
                iamRole = iamClient.getRole(new GetRoleRequest().withRoleName(roleName)).getRole();
            }

            BucketPolicy bucketPolicy =new BucketPolicy()
                        .withVersion("2012-10-17")
                        .withId("ecs-sync-s3-test-policy")
                        .withStatements(Arrays.asList(
                                new BucketPolicyStatement()
                                        .withSid("alt-user-object")
                                        .withPrincipal("{\"AWS\":\"" + iamRole.getArn() + "\"}")
                                        .withEffect(BucketPolicyStatement.Effect.Allow)
                                        .withActions(BucketPolicyAction.All)
                                        .withResource("arn:aws:s3:::" + getTestBucket() + "/*"),
                                new BucketPolicyStatement()
                                        .withSid("alt-user-bucket")
                                        .withPrincipal("{\"AWS\":\"" + iamRole.getArn() + "\"}")
                                        .withEffect(BucketPolicyStatement.Effect.Allow)
                                        .withActions(BucketPolicyAction.All)
                                        .withResource("arn:aws:s3:::" + getTestBucket())
                        ));
            ecsS3.setBucketPolicy(getTestBucket(),bucketPolicy);
        }
    }

    @AfterEach
    public void teardown() {
        if (testStorage != null) testStorage.close();
        if (storage != null) storage.close();
        if (ecsS3 != null) ecsS3.destroy();
        if (alternateUser != null) {
            // must delete access keys first
            for (AccessKeyMetadata accessKeyMeta : iamClient.listAccessKeys(new ListAccessKeysRequest().withUserName(alternateUser.getUserName())).getAccessKeyMetadata()) {
                iamClient.deleteAccessKey(new DeleteAccessKeyRequest(alternateUser.getUserName(), accessKeyMeta.getAccessKeyId()));
            }
            iamClient.deleteUser(new DeleteUserRequest(alternateUser.getUserName()));
        }
	if (iamRole != null) iamClient.deleteRole(new DeleteRoleRequest().withRoleName(iamRole.getRoleName()));
    }

    @Test
    public void testNormalUpload() {
        String key = "normal-upload";
        long size = 512 * 1024; // 512KiB
        InputStream stream = new RandomInputStream(size);
        SyncObject object = new SyncObject(storage, key, new ObjectMetadata().withContentLength(size), stream, null);

        storage.updateObject(key, object);

        // proper ETag means no MPU was performed
        Assertions.assertEquals(object.getMd5Hex(true).toUpperCase(), ecsS3.getObjectMetadata(getTestBucket(), key).getETag().toUpperCase());
    }

    @Test
    public void testCifsEcs() {
        String sBucket = "ecs-sync-cifs-ecs-test";
        String key = "empty-file";
        ecsS3.createBucket(sBucket);
        ecsS3.putObject(sBucket, key, (Object) null, null);

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
            TestUtil.run(sync);

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
            Assertions.assertNotNull(ecsS3.getObjectMetadata(getTestBucket(), key));
        } finally {
            ecsS3.deleteObject(sBucket, key);
            ecsS3.deleteBucket(sBucket);
        }
    }

    @Test
    public void testCrazyCharacters() {
        String[] keys = {
                "foo!@#$%^&*()-_=+",
                "bar\u00a1\u00bfbar",
                "查找的unicode",
                "baz\u0007bim",
                "bim\u0006-boozle"
        };

        for (String key : keys) {
            ecsS3.putObject(getTestBucket(), key, (Object) null, null);
        }

        EcsS3Config s3Config = generateConfig();
        s3Config.setApacheClientEnabled(true);
        s3Config.setUrlEncodeKeys(true);

        SyncConfig config = new SyncConfig().withSource(s3Config).withTarget(new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false));
        config.getOptions().withVerify(true).setRetryAttempts(0); // disable retries for brevity

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(config);
        TestUtil.run(sync);

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(keys.length, sync.getStats().getObjectsComplete());
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
        Assertions.assertTrue(ecsS3.getObjectMetadata(getTestBucket(), key).getETag().contains("-"));

        // verify bytes read from source
        // first wait a tick so the perf counter has at least one interval
        Thread.sleep(1000);
        Assertions.assertEquals(size, object.getBytesRead());
        Assertions.assertTrue(testStorage.getReadRate() > 0);

        // need to read the entire object since we can't use the ETag
        InputStream objectStream = ecsS3.getObject(getTestBucket(), key).getObject();
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
        StreamUtil.copy(stream, Files.newOutputStream(tempFile.toPath()), size);

        SyncObject object = new SyncObject(testStorage, key, new ObjectMetadata().withContentLength(size), Files.newInputStream(tempFile.toPath()), null);
        object.setProperty(AbstractFilesystemStorage.PROP_FILE, tempFile);

        storage.updateObject(key, object);

        // hyphen denotes an MPU
        Assertions.assertTrue(ecsS3.getObjectMetadata(getTestBucket(), key).getETag().contains("-"));

        // verify bytes read from source
        // first wait a tick so the perf counter has at least one interval
        Thread.sleep(1000);
        Assertions.assertEquals(size, object.getBytesRead());
        Assertions.assertTrue(testStorage.getReadRate() > 0);

        // need to read the entire object since we can't use the ETag
        InputStream objectStream = ecsS3.getObject(getTestBucket(), key).getObject();
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

        Assertions.assertEquals(RestUtil.DEFAULT_CONTENT_TYPE, ecsS3.getObjectMetadata(getTestBucket(), key).getContentType());
    }

    @Test
    public void testTemporaryCredentials() {
        // must have an STS endpoint
        Assumptions.assumeTrue(getStsEndpoint() != null);

        EcsS3Config s3Config = storage.getConfig();
        com.emc.ecs.sync.config.storage.TestConfig testConfig = testStorage.getConfig();
        testConfig.setDiscardData(false); // make sure we keep track of objects

        // use AWS SDK to get session token (until this is supported in ECS SDK)
        AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(getStsEndpoint(), getS3Region()))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        s3Config.getAccessKey(), s3Config.getSecretKey()))).build();
	AssumeRoleResult assumeRoleResult = sts.assumeRole(new AssumeRoleRequest()
                .withRoleSessionName("ecs-sync-testTemporaryCredentials").withRoleArn(iamRole.getArn()));
        Credentials sessionCreds = assumeRoleResult.getCredentials();

        // set credentials on storage config
        s3Config.setAccessKey(sessionCreds.getAccessKeyId());
        s3Config.setSecretKey(sessionCreds.getSecretAccessKey());
        s3Config.setSessionToken(sessionCreds.getSessionToken());

        // test as target

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withTarget(s3Config));
        sync.setSource(testStorage);
        TestUtil.run(sync);

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
        TestUtil.run(sync);

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(totalGeneratedObjects, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(totalGeneratedObjects, testStorage.getTotalObjectCount());
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
            EcsS3Config ecsS3Config = storage.getConfig();
            ecsS3Config.setRetentionType(EcsRetentionType.Classic);
            retentionStorage.setConfig(ecsS3Config);
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
            Assertions.assertTrue(isRetentionInRange(retentionPeriod, ecsS3.getObjectMetadata(getTestBucket(), key).getRetentionPeriod()));
        } finally {
            // in case any assertions fail or there is any other error, we need to wait for retention to expire before trying to delete the object
            Thread.sleep(15_000);
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
        } catch (S3Exception e) {
            Assertions.assertEquals(400, e.getHttpCode());
            Assertions.assertTrue(e.getMessage().contains("Content-MD5"));
        }

        // make sure bad object does not exist
        try {
            ecsS3.getObjectMetadata(getTestBucket(), badKey);
        } catch (S3Exception e) {
            Assertions.assertEquals(404, e.getHttpCode());
        }
    }

    @Test
    public void testSkipExistingSameTarget() {
        // the test is for non-MPU objects only
        String key = "ecs-sync-test-existing-object";
        String data = "Dummy data to generate an MD5.";
        PutObjectRequest request = new PutObjectRequest(getTestBucket(), key, data)
                .withObjectMetadata(new S3ObjectMetadata().withContentLength(data.length()).withContentType("text/plain"));
        ecsS3.putObject(request);
        Date lastModified = ecsS3.getObjectMetadata(getTestBucket(), key).getLastModified();
        String eTag = ecsS3.getObjectMetadata(getTestBucket(), key).getETag();

        SyncConfig syncConfig = new SyncConfig().withTarget(storage.getConfig()).withSource(testStorage.getConfig().withDiscardData(false));
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        sync.setTarget(storage);

        testStorage.createObject(new SyncObject(testStorage, key,
                new ObjectMetadata().withContentLength(data.length()).withContentType("text/plain").withHttpEtag(eTag),
                new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), null));

        storage.getOptions().setSyncMetadata(false);
        TestUtil.run(sync);
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
        // target should not get modified because Etag for If-None-Match PUT is of the same
        Assertions.assertEquals(lastModified, ecsS3.getObjectMetadata(getTestBucket(), key).getLastModified());

        storage.getOptions().setSyncMetadata(true);
        sync = new EcsSync(); // cannot reuse an instance
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        sync.setTarget(storage);
        TestUtil.run(sync);

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
        // Metadata gets updated while data writing is skipped(verified in previous test)
        Assertions.assertTrue(lastModified.before(ecsS3.getObjectMetadata(getTestBucket(), key).getLastModified()));

        lastModified = ecsS3.getObjectMetadata(getTestBucket(), key).getLastModified();
        storage.getOptions().setSyncMetadata(false);
        storage.getOptions().setForceSync(true);
        sync = new EcsSync(); // cannot reuse an instance
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        sync.setTarget(storage);
        TestUtil.run(sync);

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
        // force Sync should overwrite existing object
        Assertions.assertTrue(lastModified.before(ecsS3.getObjectMetadata(getTestBucket(), key).getLastModified()));
    }

    @Test
    public void testSafeDeletion() {
        String ecsVersion = ecsS3.listDataNodes().getVersionInfo();
        Assumptions.assumeTrue(ecsVersion != null && ecsVersion.compareTo("3.7.1") >= 0, "ECS version must be at least 3.7.1 or above");

        String key = "safe-deletion-test";
        ecsS3.putObject(getTestBucket(), key, "Original Content", "text/plain");
        SyncObject object = storage.loadObject(key);
        Assertions.assertNotNull(object.getMetadata().getHttpEtag());
        Assertions.assertEquals(object.getMetadata().getHttpEtag(), ecsS3.getObjectMetadata(getTestBucket(), key).getETag());

        ecsS3.putObject(getTestBucket(), key, "Modified Content", "text/plain");
        Assertions.assertNotEquals(object.getMetadata().getHttpEtag(), ecsS3.getObjectMetadata(getTestBucket(), key).getETag());

        // object shouldn't get deleted because Etag for If-Match is out-dated.
        storage.delete(key, object);
        try {
            object = storage.loadObject(key);
        } catch (ObjectNotFoundException e) {
            Assertions.fail("Source Object has been modified during sync, deletion is expected to fail.");
        }

        Assertions.assertNotNull(object.getMetadata().getHttpEtag());
        Assertions.assertEquals(object.getMetadata().getHttpEtag(), ecsS3.getObjectMetadata(getTestBucket(), key).getETag());
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
            ecsS3.setBucketPolicy(getTestBucket(), getAltUserReadPolicy());
            storage.getConfig().setAccessKey(altUserAccessKey);
            storage.getConfig().setSecretKey(altUserSecretKey);
            storage.configure(null, null, storage);
            Assertions.fail("expected 403");
        } catch (S3ConfigurationException e) {
            Assertions.assertEquals(S3ConfigurationException.Error.ERROR_BUCKET_ACCESS_WRITE, e.getError());
        } finally {
            ecsS3.deleteBucketPolicy(getTestBucket());
        }

        storage.getConfig().setAccessKey(origAccessKey);
        storage.getConfig().setSecretKey(origSecretKey);
        storage.configure(null, null, storage);

        // verify no object was written to the bucket
        Assertions.assertEquals(0, ecsS3.listObjects(getTestBucket()).getObjects().size());
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
        int delayMs = 4950; // putObject method uses 5-second intervals to check for job termination - this should be just under that
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
                .withOptions(new SyncOptions());
        // this will delay the first read of the data stream
        syncConfig.withFilters(Arrays.asList(startNotifyConfig, new DelayFilter.DelayConfig().withDataStreamDelayMs(delayMs)));

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        sync.setTarget(storage);

        // ingest the object into the source first
        // NOTE: test objects are not multi-part sources, so MPUs will be sequential, not parallel
        testStorage.getConfig().withDiscardData(false);
        testStorage.createObject(new SyncObject(testStorage, key,
                new ObjectMetadata().withContentLength(mockMultipartSource.getTotalSize())
                        .withContentType("text/plain"),
                mockMultipartSource.getCompleteDataStream(), null));

        CompletableFuture<?> future = CompletableFuture.runAsync(sync);
        startNotifyConfig.waitForStart();
        Thread.sleep(delayMs); // need to terminate after the first part is started, but before it finishes
        sync.terminate();
        future.get();

        if (enableResume) {
            // object should not exist
            Assertions.assertThrows(S3Exception.class, () -> ecsS3.getObjectMetadata(getTestBucket(), key));
            // MPU should exist
            ListMultipartUploadsResult result = ecsS3.listMultipartUploads(new ListMultipartUploadsRequest(getTestBucket()).withPrefix(key));
            Assertions.assertEquals(1, result.getUploads().size());
            // MPU should have only 1 part
            String uploadId = result.getUploads().get(0).getUploadId();
            Assertions.assertEquals(1, ecsS3.listParts(getTestBucket(), key, uploadId).getParts().size());

            EcsSync sync2 = new EcsSync();
            sync2.setSyncConfig(syncConfig);
            sync2.setSource(testStorage);
            sync2.setTarget(storage);

            TestUtil.run(sync2);
            Assertions.assertEquals(1, sync2.getStats().getObjectsComplete());
        } else {
            Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
        }

        Assertions.assertEquals(0, ecsS3.listMultipartUploads(new ListMultipartUploadsRequest(getTestBucket()).withPrefix(key)).getUploads().size());
        S3ObjectMetadata metadata = ecsS3.getObjectMetadata(getTestBucket(), key);
        Assertions.assertEquals(mockMultipartSource.getMpuETag(), metadata.getETag());
        Assertions.assertEquals(mockMultipartSource.getTotalSize(), metadata.getContentLength());
    }

    @Test
    public void testMpuAbort() throws Exception {
        // only enable this when testing against a local lab cluster
        Assumptions.assumeTrue(Boolean.parseBoolean(TestConfig.getProperties().getProperty(TestConfig.PROP_LARGE_DATA_TESTS)));

        String key = "testMpuAbort";
        int delayMs = 1000; // needed for part upload synchronization (see below)
        AwsS3LargeFileUploaderTest.MockMultipartSource mockMultipartSource = new AwsS3LargeFileUploaderTest.MockMultipartSource();
        String exceptionMessage = "mpu-abort-exception-message";

        EcsS3Config targetConfig = storage.getConfig();
        targetConfig.setMpuEnabled(true);
        targetConfig.setMpuPartSizeMb((int) mockMultipartSource.getPartSize() / 1024 / 1024);
        targetConfig.setMpuThresholdMb(targetConfig.getMpuPartSizeMb());

        TestStorage testStorage = new TestStorage();
        testStorage.withConfig(new com.emc.ecs.sync.config.storage.TestConfig());

        StartNotifyFilter.StartNotifyConfig startNotifyConfig = new StartNotifyFilter.StartNotifyConfig();

        SyncConfig syncConfig = new SyncConfig()
                .withTarget(targetConfig)
                .withSource(testStorage.getConfig())
                .withOptions(new SyncOptions().withThreadCount(2));
        // this will throw an IOException *after* the first part is uploaded
        StreamErrorThrowingFilter.StreamErrorThrowingConfig errorThrowingConfig =
                new StreamErrorThrowingFilter.StreamErrorThrowingConfig(new IOException(exceptionMessage), mockMultipartSource.getPartSize());
        syncConfig.withFilters(Arrays.asList(
                startNotifyConfig,
                new DelayFilter.DelayConfig().withDataStreamDelayMs(delayMs),
                errorThrowingConfig)
        );

        // ingest the object into the source first
        testStorage.getConfig().withDiscardData(false);
        testStorage.createObject(new SyncObject(testStorage, key,
                new com.emc.ecs.sync.model.ObjectMetadata().withContentLength(mockMultipartSource.getTotalSize())
                        .withContentType("text/plain"),
                mockMultipartSource.getCompleteDataStream(), null));

        // test no-abort from IOException generated in one of the parts
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        targetConfig.setMpuResumeEnabled(true);
        TestUtil.run(sync);
        // no successes, 1 failure
        Assertions.assertEquals(0, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(1, sync.getStats().getObjectsFailed());
        // object should not exist
        try {
            ecsS3.getObjectMetadata(getTestBucket(), key);
        } catch (S3Exception e) {
            Assertions.assertEquals(404, e.getHttpCode());
        }
        // MPU should exist
        ListMultipartUploadsResult multipartUploadListing = ecsS3.listMultipartUploads(new ListMultipartUploadsRequest(getTestBucket()).withPrefix(key));
        Assertions.assertEquals(1, multipartUploadListing.getUploads().size());
        // MPU should have only 1 part
        String uploadId = multipartUploadListing.getUploads().get(0).getUploadId();
        List<MultipartPart> parts = ecsS3.listParts(getTestBucket(), key, uploadId).getParts();
        Assertions.assertEquals(1, parts.size());
        // make sure part 1 is the correct size
        Assertions.assertEquals(mockMultipartSource.getPartSize(), parts.get(0).getSize());

        // test no-abort-from-resume (same IOException)
        sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        targetConfig.setMpuResumeEnabled(true);
        errorThrowingConfig.setThrowOnThisByte(2 * mockMultipartSource.getPartSize()); // should write 2 parts before error
        TestUtil.run(sync);
        // no successes, 1 failure
        Assertions.assertEquals(0, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(1, sync.getStats().getObjectsFailed());
        // object should not exist
        try {
            ecsS3.getObjectMetadata(getTestBucket(), key);
        } catch (S3Exception e) {
            Assertions.assertEquals(404, e.getHttpCode());
        }
        // same MPU should still exist
        multipartUploadListing = ecsS3.listMultipartUploads(new ListMultipartUploadsRequest(getTestBucket()).withPrefix(key));
        Assertions.assertEquals(1, multipartUploadListing.getUploads().size());
        // MPU should have 2 parts
        parts = ecsS3.listParts(getTestBucket(), key, uploadId).getParts();
        Assertions.assertEquals(2, parts.size());
        // make sure parts are the correct size
        Assertions.assertEquals(mockMultipartSource.getPartSize(), parts.get(0).getSize());
        Assertions.assertEquals(mockMultipartSource.getPartSize(), parts.get(1).getSize());

        // clean up the MPU
        ecsS3.abortMultipartUpload(new AbortMultipartUploadRequest(getTestBucket(), key, uploadId));

        // test abort-without-resume (same IOException)
        sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        targetConfig.setMpuResumeEnabled(false);
        errorThrowingConfig.setThrowOnThisByte(mockMultipartSource.getPartSize()); // should write 1 part before error
        TestUtil.run(sync);
        // no successes, 1 failure
        Assertions.assertEquals(0, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(1, sync.getStats().getObjectsFailed());
        // object should not exist
        try {
            ecsS3.getObjectMetadata(getTestBucket(), key);
        } catch (S3Exception e) {
            Assertions.assertEquals(404, e.getHttpCode());
        }
        // MPU should not exist (should have been aborted)
        multipartUploadListing = ecsS3.listMultipartUploads(new ListMultipartUploadsRequest(getTestBucket()).withPrefix(key));
        Assertions.assertEquals(0, multipartUploadListing.getUploads().size());
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
                                .withResource("arn:aws:s3:::" + getTestBucket() + "/*"),
                        new BucketPolicyStatement()
                                .withSid("alt-user-head-bucket")
                                .withPrincipal("{\"AWS\":\"" + alternateUser.getArn() + "\"}")
                                .withEffect(BucketPolicyStatement.Effect.Allow)
                                .withActions(BucketPolicyAction.ListBucket)
                                .withResource("arn:aws:s3:::" + getTestBucket())
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
        TestUtil.run(sync);
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
        TestUtil.run(sync);

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(tpsLimit, sync.getStats().getObjectCompleteRate(), DELTA_TPS);
    }

    @Test
    public void testSyncObjectLockRetention() throws Exception{
        //The test requires IAM user, non-empty alternateUser ensures the access key is from a IAM user.
        Assumptions.assumeTrue(alternateUser != null);

        String srcBucket = "ecs-sync-src-retention-bucket";
        String dstBucket = "ecs-sync-dst-object-lock-bucket";
        String key1 = "retention-file";
        String key2 = "object-lock-file";
        Long retention_period = 10L;

        ecsS3.createBucket(srcBucket);
        //bucket versioning will be enabled when Object Lock is enabled
        ecsS3.enableObjectLock(srcBucket);

        //Create object #1 version #1, wait for retention to expire
        PutObjectRequest request = new PutObjectRequest(srcBucket, key1, "Expired Retention")
                .withObjectMetadata(new S3ObjectMetadata().withRetentionPeriod(1L));
        ecsS3.putObject(request);
        Thread.sleep(1000);
        //Create version object #1 version #2 with unexpired retention
        request = new PutObjectRequest(srcBucket, key1, "Unexpired Retention")
                .withObjectMetadata(new S3ObjectMetadata().withRetentionPeriod(retention_period));
        PutObjectResult putObjectResult = ecsS3.putObject(request);

        List<AbstractVersion> srcVersions = ecsS3.listVersions(srcBucket, key1).getVersions();
        Assertions.assertEquals(2, srcVersions.size());

        //Create object #2, S3 Object Lock with Legal Hold ON and 10 minutes' retention
        Date retainUntilDate = new Date(System.currentTimeMillis() + 10 * 60 * 1000);
        request = new PutObjectRequest(srcBucket, key2, "S3 Object Lock")
                .withObjectMetadata(new S3ObjectMetadata().withObjectLockRetention(new ObjectLockRetention()
                                .withMode(ObjectLockRetentionMode.GOVERNANCE).withRetainUntilDate(retainUntilDate))
                        .withObjectLockLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHold.Status.ON)));
        ecsS3.putObject(request);

        try {
            EcsS3Config source = storage.getConfig();
            source.setBucketName(srcBucket);
            source.setIncludeVersions(true);

            EcsS3Config target = new EcsS3Config();
            target.setProtocol(source.getProtocol());
            target.setHost(source.getHost());
            target.setPort(source.getPort());
            target.setEnableVHosts(source.isEnableVHosts());
            target.setAccessKey(source.getAccessKey());
            target.setSecretKey(source.getSecretKey());
            target.setBucketName(dstBucket);
            target.setCreateBucket(true);
            target.setIncludeVersions(true);
            target.setRetentionType(EcsRetentionType.ObjectLock);
            target.setDefaultRetentionMode(RetentionMode.Governance);

            EcsS3Storage targetStorage = new EcsS3Storage();
            targetStorage.setConfig(target);

            SyncConfig config = new SyncConfig().withSource(source).withTarget(target);
            config.getOptions().setRetryAttempts(0); // disable retries for brevity
            config.getOptions().setSyncRetentionExpiration(true);
            config.getOptions().setSyncMetadata(true);

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(config);
            sync.run();

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(2, sync.getStats().getObjectsComplete());

            Assertions.assertEquals(ObjectLockConfiguration.ObjectLockEnabled.Enabled, ecsS3.getObjectLockConfiguration(dstBucket).getObjectLockEnabled());

            List<AbstractVersion> dstVersions = ecsS3.listVersions(dstBucket, key1).getVersions();
            Assertions.assertEquals(2, dstVersions.size());
            //Verify Object #1 version #1 (expired classic retention), Object Lock Retention should not be set
            GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(dstBucket, key1).withVersionId(dstVersions.get(1).getVersionId());
            S3ObjectMetadata om = ecsS3.getObjectMetadata(getObjectMetadataRequest);
            Assertions.assertNull(om.getObjectLockRetention());
            Assertions.assertNull(om.getObjectLockLegalHold());

            //Verify Object #1 version #2 (classic retention)
            om = ecsS3.getObjectMetadata(getObjectMetadataRequest.withVersionId(dstVersions.get(0).getVersionId()));
            S3ObjectMetadata om2 = ecsS3.getObjectMetadata(new GetObjectMetadataRequest(srcBucket, key1).withVersionId(putObjectResult.getVersionId()));
            Assertions.assertEquals(om2.getLastModified().getTime() + retention_period * 1000,   om.getObjectLockRetention().getRetainUntilDate().getTime());
            //Retention Mode should match DefaultRetentionMode set in target config when syncing from classic retention
            Assertions.assertEquals(ObjectLockRetentionMode.GOVERNANCE, om.getObjectLockRetention().getMode());
            Assertions.assertNull(om.getObjectLockLegalHold());

            //Verify Object #2 (S3 Object Lock)
            om = ecsS3.getObjectMetadata(new GetObjectMetadataRequest(dstBucket, key2));
            //Default Retention MOde is Governance
            Assertions.assertEquals(ObjectLockRetentionMode.GOVERNANCE, om.getObjectLockRetention().getMode());
            Assertions.assertEquals(retainUntilDate.getTime(), om.getObjectLockRetention().getRetainUntilDate().getTime());
            Assertions.assertEquals(om.getObjectLockLegalHold().getStatus(), ObjectLockLegalHold.Status.ON);

        }
        finally {
            Thread.sleep(retention_period * 1000);
            for (AbstractVersion version :  ecsS3.listVersions(srcBucket, key1).getVersions()) {
                ecsS3.deleteVersion(srcBucket, key1, version.getVersionId());
            }
            for (AbstractVersion version :  ecsS3.listVersions(srcBucket, key2).getVersions()) {
                ecsS3.setObjectLegalHold(new SetObjectLegalHoldRequest(srcBucket, key2).withVersionId(version.getVersionId())
                        .withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHold.Status.OFF)));
                ecsS3.deleteObject(new DeleteObjectRequest(srcBucket, key2).withVersionId(version.getVersionId())
                        .withBypassGovernanceRetention(true));
            }
            ecsS3.deleteBucket(srcBucket);

            for (AbstractVersion version : ecsS3.listVersions(dstBucket, key1).getVersions()) {
                ecsS3.deleteVersion(dstBucket, key1, version.getVersionId());
            }
            for (AbstractVersion version :  ecsS3.listVersions(dstBucket, key2).getVersions()) {
                ecsS3.setObjectLegalHold(new SetObjectLegalHoldRequest(dstBucket, key2).withVersionId(version.getVersionId())
                        .withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHold.Status.OFF)));
                ecsS3.deleteObject(new DeleteObjectRequest(dstBucket, key2).withVersionId(version.getVersionId())
                        .withBypassGovernanceRetention(true));
            }
            ecsS3.deleteBucket(dstBucket);
        }
    }
}

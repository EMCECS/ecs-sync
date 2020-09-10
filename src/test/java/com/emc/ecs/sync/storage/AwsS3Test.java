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
package com.emc.ecs.sync.storage;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsSyncClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;
import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.AwsS3Config;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.file.AbstractFilesystemStorage;
import com.emc.ecs.sync.storage.s3.AwsS3Storage;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.util.RandomInputStream;
import com.emc.object.util.ChecksumAlgorithm;
import com.emc.object.util.ChecksummedInputStream;
import com.emc.object.util.RunningChecksum;
import com.emc.rest.util.StreamUtil;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AwsS3Test {
    private static final Logger log = LoggerFactory.getLogger(AwsS3Test.class);

    private String endpoint;
    private URI endpointUri;
    private String accessKey;
    private String secretKey;
    private String region;
    private AmazonS3 s3;

    @Before
    public void setup() throws Exception {
        Properties syncProperties = TestConfig.getProperties();
        endpoint = syncProperties.getProperty(TestConfig.PROP_S3_ENDPOINT);
        accessKey = syncProperties.getProperty(TestConfig.PROP_S3_ACCESS_KEY_ID);
        secretKey = syncProperties.getProperty(TestConfig.PROP_S3_SECRET_KEY);
        region = syncProperties.getProperty(TestConfig.PROP_S3_REGION);
        String proxyUri = syncProperties.getProperty(TestConfig.PROP_HTTP_PROXY_URI);
        Assume.assumeNotNull(endpoint, accessKey, secretKey);
        endpointUri = new URI(endpoint);

        ClientConfiguration config = new ClientConfiguration().withSignerOverride("S3SignerType");
        if (proxyUri != null) {
            URI uri = new URI(proxyUri);
            config.setProxyHost(uri.getHost());
            config.setProxyPort(uri.getPort());
        }

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withClientConfiguration(config)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));

        s3 = builder.build();
    }

    @Test
    public void testVersions() throws Exception {
        String bucket1 = "ecs-sync-s3-test-versions";
        String bucket2 = "ecs-sync-s3-test-versions-2";

        log.info("creating buckets with versioning enabled...");
        createBucket(bucket1, true);
        createBucket(bucket2, true);

        try {
            SyncOptions options = new SyncOptions().withThreadCount(32).withRetryAttempts(0).withForceSync(true).withVerify(true);

            // test data
            com.emc.ecs.sync.config.storage.TestConfig testConfig = new com.emc.ecs.sync.config.storage.TestConfig();
            testConfig.withDiscardData(false).withReadData(true).withObjectCount(50).withMaxSize(10 * 1024);

            AwsS3Config s3Config1 = new AwsS3Config();
            if (endpointUri.getScheme() != null)
                s3Config1.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
            s3Config1.setHost(endpointUri.getHost());
            s3Config1.setPort(endpointUri.getPort());
            s3Config1.setAccessKey(accessKey);
            s3Config1.setSecretKey(secretKey);
            s3Config1.setRegion(region);
            s3Config1.setLegacySignatures(true);
            s3Config1.setDisableVHosts(true);
            s3Config1.setBucketName(bucket1);
            s3Config1.setPreserveDirectories(true);

            SyncConfig syncConfig = new SyncConfig().withOptions(options);
            syncConfig.setTarget(s3Config1);

            // push it into bucket1
            log.info("writing v1 source data...");
            TestStorage testSource = new TestStorage();
            testSource.withConfig(testConfig).withOptions(options);
            runSync(syncConfig, testSource);

            // 2nd version is delete
            log.info("deleting source objects (for v2)...");
            deleteObjects(bucket1);

            // 3rd version is altered
            log.info("writing v3 source data...");
            TestStorage testSource2 = new TestStorage();
            testSource2.withConfig(testConfig).withOptions(options);
            testSource2.ingest(testSource, null);
            alterContent(testSource2, null, "3");
            runSync(syncConfig, testSource2);
            testSource = testSource2;

            // 4th version is altered again
            log.info("writing v4 source data...");
            testSource2 = new TestStorage();
            testSource2.withConfig(testConfig).withOptions(options);
            testSource2.ingest(testSource, null);
            alterContent(testSource2, null, "4");
            runSync(syncConfig, testSource2);

            // now run migration to bucket2
            s3Config1.setIncludeVersions(true);
            options.withForceSync(false);
            options.setSyncAcl(true);

            AwsS3Config s3Config2 = new AwsS3Config();
            if (endpointUri.getScheme() != null)
                s3Config2.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
            s3Config2.setHost(endpointUri.getHost());
            s3Config2.setPort(endpointUri.getPort());
            s3Config2.setAccessKey(accessKey);
            s3Config2.setSecretKey(secretKey);
            s3Config2.setRegion(region);
            s3Config2.setLegacySignatures(true);
            s3Config2.setDisableVHosts(true);
            s3Config2.setBucketName(bucket2);
            s3Config2.setCreateBucket(true);
            s3Config2.setIncludeVersions(true);
            s3Config2.setPreserveDirectories(true);

            syncConfig.setSource(s3Config1);
            syncConfig.setTarget(s3Config2);

            log.info("migrating versions to bucket2...");
            runSync(syncConfig, null);

            // verify all versions
            log.info("verifying versions...");
            verifyBuckets(bucket1, bucket2);

            // test verify only sync
            log.info("syncing with verify-only...");
            options.withVerify(false).withVerifyOnly(true);
            runSync(syncConfig, null);

            // add v5 (delete) to bucket1 (testing delete)
            log.info("deleting objects in source (for v5)...");
            deleteObjects(bucket1);

            // test deleted objects
            log.info("migrating v5 to target...");
            options.withVerifyOnly(false).withVerify(true);
            runSync(syncConfig, null);
            log.info("verifying versions...");
            verifyBuckets(bucket1, bucket2);

            // test deleted objects from scratch
            log.info("wiping bucket2...");
            deleteVersionedBucket(bucket2);
            log.info("migrating versions again from scratch...");
            runSync(syncConfig, null);
            log.info("verifying versions...");
            verifyBuckets(bucket1, bucket2);

            // test verify-only with deleted objects
            log.info("syncing with verify-only (all delete markers)...");
            options.withVerify(false).withVerifyOnly(true);
            runSync(syncConfig, null);

        } finally {
            log.info("cleaning up bucket1...");
            deleteVersionedBucket(bucket1);
            log.info("cleaning up bucket2...");
            deleteVersionedBucket(bucket2);
        }
    }

    @Test
    public void testSetAcl() throws Exception {
        String bucket = "ecs-sync-s3-test-acl";
        String key = "test-object";
        createBucket(bucket, true);

        try {
            String content = "hello ACLs";

            s3.putObject(bucket, key, new ByteArrayInputStream(content.getBytes()), null); // 1st version

            AccessControlList acl = new AccessControlList();
            acl.setOwner(new Owner(accessKey, accessKey));
            acl.grantPermission(new CanonicalGrantee(accessKey), Permission.FullControl);
            acl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Read);
            acl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Write);
            acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);

            PutObjectRequest putRequest = new PutObjectRequest(bucket, key, new ByteArrayInputStream(content.getBytes()), null);
            putRequest.setAccessControlList(acl);

            s3.putObject(putRequest); // 2nd version

            AccessControlList remoteAcl = s3.getObjectAcl(bucket, key);

            verifyAcls(acl, remoteAcl);
        } finally {
            try {
                deleteVersionedBucket(bucket);
            } catch (Throwable t) {
                log.warn("could not delete bucket: " + t.getMessage());
            }
        }
    }

    @Test
    public void testSyncVersionsWithAcls() throws Exception {
        String bucket1 = "ecs-sync-s3-test-sync-acl1";
        String bucket2 = "ecs-sync-s3-test-sync-acl2";
        createBucket(bucket1, true);

        String key1 = "key1", key2 = "key2", key3 = "key3";

        AccessControlList largeAcl = new AccessControlList();
        largeAcl.setOwner(new Owner(accessKey, accessKey));
        largeAcl.grantPermission(new CanonicalGrantee(accessKey), Permission.FullControl);
        largeAcl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Read);
        largeAcl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Write);
        largeAcl.grantPermission(GroupGrantee.AllUsers, Permission.Read);

        AccessControlList midAcl = new AccessControlList();
        midAcl.setOwner(new Owner(accessKey, accessKey));
        midAcl.grantPermission(new CanonicalGrantee(accessKey), Permission.FullControl);
        midAcl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Read);

        AccessControlList defaultAcl = new AccessControlList();
        defaultAcl.setOwner(new Owner(accessKey, accessKey));
        defaultAcl.grantPermission(new CanonicalGrantee(accessKey), Permission.FullControl);

        try {
            // default acls
            s3.putObject(bucket1, key1, new ByteArrayInputStream("data1".getBytes()), null);
            s3.putObject(bucket1, key1, new ByteArrayInputStream("data1".getBytes()), null);
            s3.putObject(bucket1, key1, new ByteArrayInputStream("data1".getBytes()), null);

            // default acl on latest
            PutObjectRequest request = new PutObjectRequest(bucket1, key2, new ByteArrayInputStream("data2".getBytes()), null);
            request.setAccessControlList(largeAcl);
            s3.putObject(request);
            request = new PutObjectRequest(bucket1, key2, new ByteArrayInputStream("data2".getBytes()), null);
            request.setAccessControlList(midAcl);
            s3.putObject(request);
            s3.putObject(bucket1, key2, new ByteArrayInputStream("data2".getBytes()), null);

            // default acl on first version
            s3.putObject(bucket1, key3, new ByteArrayInputStream("data3".getBytes()), null);
            request = new PutObjectRequest(bucket1, key3, new ByteArrayInputStream("data3".getBytes()), null);
            request.setAccessControlList(midAcl);
            s3.putObject(request);
            request = new PutObjectRequest(bucket1, key3, new ByteArrayInputStream("data3".getBytes()), null);
            request.setAccessControlList(largeAcl);
            s3.putObject(request);

            AwsS3Config sourceConfig = new AwsS3Config();
            if (endpointUri.getScheme() != null)
                sourceConfig.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
            sourceConfig.setHost(endpointUri.getHost());
            sourceConfig.setPort(endpointUri.getPort());
            sourceConfig.setAccessKey(accessKey);
            sourceConfig.setSecretKey(secretKey);
            sourceConfig.setRegion(region);
            sourceConfig.setLegacySignatures(true);
            sourceConfig.setDisableVHosts(true);
            sourceConfig.setBucketName(bucket1);
            sourceConfig.setIncludeVersions(true);

            AwsS3Config targetConfig = new AwsS3Config();
            if (endpointUri.getScheme() != null)
                targetConfig.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
            targetConfig.setHost(endpointUri.getHost());
            targetConfig.setPort(endpointUri.getPort());
            targetConfig.setAccessKey(accessKey);
            targetConfig.setSecretKey(secretKey);
            targetConfig.setRegion(region);
            targetConfig.setLegacySignatures(true);
            targetConfig.setDisableVHosts(true);
            targetConfig.setBucketName(bucket2);
            targetConfig.setCreateBucket(true);
            targetConfig.setIncludeVersions(true);

            SyncOptions options = new SyncOptions().withThreadCount(1).withVerify(true).withSyncAcl(true);

            SyncConfig syncConfig = new SyncConfig().withOptions(options).withSource(sourceConfig).withTarget(targetConfig);

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.run();

            Assert.assertEquals(0, sync.getStats().getObjectsFailed());

            List<S3VersionSummary> key1Versions = getVersions(bucket2, key1);
            verifyAcls(defaultAcl, s3.getObjectAcl(bucket2, key1, key1Versions.get(0).getVersionId()));
            verifyAcls(defaultAcl, s3.getObjectAcl(bucket2, key1, key1Versions.get(1).getVersionId()));
            verifyAcls(defaultAcl, s3.getObjectAcl(bucket2, key1, key1Versions.get(2).getVersionId()));

            List<S3VersionSummary> key2Versions = getVersions(bucket2, key2);
            verifyAcls(largeAcl, s3.getObjectAcl(bucket2, key2, key2Versions.get(0).getVersionId()));
            verifyAcls(midAcl, s3.getObjectAcl(bucket2, key2, key2Versions.get(1).getVersionId()));
            verifyAcls(defaultAcl, s3.getObjectAcl(bucket2, key2, key2Versions.get(2).getVersionId()));

            List<S3VersionSummary> key3Versions = getVersions(bucket2, key3);
            verifyAcls(defaultAcl, s3.getObjectAcl(bucket2, key3, key3Versions.get(0).getVersionId()));
            verifyAcls(midAcl, s3.getObjectAcl(bucket2, key3, key3Versions.get(1).getVersionId()));
            verifyAcls(largeAcl, s3.getObjectAcl(bucket2, key3, key3Versions.get(2).getVersionId()));
        } finally {
            deleteVersionedBucket(bucket1);
            deleteVersionedBucket(bucket2);
        }
    }

    @Test
    public void testNormalUpload() throws Exception {
        String bucketName = "ecs-sync-s3-target-test-bucket";
        createBucket(bucketName, false);

        TestStorage source = new TestStorage();
        source.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).withOptions(new SyncOptions());
        AwsS3Storage s3Target = null;
        try {
            String key = "normal-upload";
            long size = 512 * 1024; // 512KiB
            InputStream stream = new RandomInputStream(size);
            SyncObject object = new SyncObject(source, key, new ObjectMetadata().withContentLength(size), stream, null);

            AwsS3Config s3Config = new AwsS3Config();
            if (endpointUri.getScheme() != null)
                s3Config.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
            s3Config.setHost(endpointUri.getHost());
            s3Config.setPort(endpointUri.getPort());
            s3Config.setAccessKey(accessKey);
            s3Config.setSecretKey(secretKey);
            s3Config.setRegion(region);
            s3Config.setLegacySignatures(true);
            s3Config.setDisableVHosts(true);
            s3Config.setBucketName(bucketName);

            s3Target = new AwsS3Storage();
            s3Target.withConfig(s3Config).withOptions(new SyncOptions());
            s3Target.configure(source, null, s3Target);

            String createdKey = s3Target.createObject(object);

            Assert.assertEquals(key, createdKey);

            // verify bytes read from source
            // first wait a tick so the perf counter has at least one interval
            Thread.sleep(1000);
            Assert.assertEquals(size, object.getBytesRead());
            Assert.assertTrue(source.getReadRate() > 0);

            // proper ETag means no MPU was performed
            Assert.assertEquals(object.getMd5Hex(true).toLowerCase(), s3.getObjectMetadata(bucketName, key).getETag().toLowerCase());
        } finally {
            source.close();
            if (s3Target != null) s3Target.close();
            deleteObjects(bucketName);
            s3.deleteBucket(bucketName);
        }
    }

    @Test
    public void testNormalUploadSTS() throws Exception {
        String bucketName = "ecs-sync-s3-target-test-bucket-" + System.currentTimeMillis();
        createBucket(bucketName, false);

        TestStorage source = new TestStorage();
        source.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).withOptions(new SyncOptions());
        AwsS3Storage s3Target = null;
        try {
            String key = "normal-upload";
            long size = 512 * 1024; // 512KiB
            InputStream stream = new RandomInputStream(size);
            SyncObject object = new SyncObject(source, key, new ObjectMetadata().withContentLength(size), stream, null);

            AwsS3Config s3Config = new AwsS3Config();
            if (endpointUri.getScheme() != null)
                s3Config.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
            s3Config.setHost(endpointUri.getHost());
            s3Config.setPort(endpointUri.getPort());

            final AWSSecurityTokenService awsSecurityTokenService = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();
            GetSessionTokenRequest sessionTokenRequest = new GetSessionTokenRequest();
            sessionTokenRequest.setDurationSeconds(7200);
            final GetSessionTokenResult sessionTokenResult = awsSecurityTokenService.getSessionToken(sessionTokenRequest);
            final Credentials stsCredentials = sessionTokenResult.getCredentials();

            s3Config.setAccessKey(stsCredentials.getAccessKeyId());
            s3Config.setSecretKey(stsCredentials.getSecretAccessKey());
            s3Config.setSessionToken(stsCredentials.getSessionToken());
            s3Config.setRegion(region);
            s3Config.setLegacySignatures(true);
            s3Config.setDisableVHosts(true);
            s3Config.setBucketName(bucketName);

            s3Target = new AwsS3Storage();
            s3Target.withConfig(s3Config).withOptions(new SyncOptions());
            s3Target.configure(source, null, s3Target);

            String createdKey = s3Target.createObject(object);

            Assert.assertEquals(key, createdKey);

            // verify bytes read from source
            // first wait a tick so the perf counter has at least one interval
            Thread.sleep(1000);
            Assert.assertEquals(size, object.getBytesRead());
            Assert.assertTrue(source.getReadRate() > 0);

            // proper ETag means no MPU was performed
            Assert.assertEquals(object.getMd5Hex(true).toLowerCase(), s3.getObjectMetadata(bucketName, key).getETag().toLowerCase());
        } finally {
            source.close();
            if (s3Target != null) s3Target.close();
            deleteObjects(bucketName);
            s3.deleteBucket(bucketName);
        }
    }

    @Ignore // only perform this test on a co-located S3 store!
    @Test
    public void testVeryLargeUploadStream() throws Exception {
        String bucketName = "ecs-sync-s3-target-test-bucket";
        createBucket(bucketName, false);

        TestStorage source = new TestStorage();
        source.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).withOptions(new SyncOptions());
        AwsS3Storage s3Target = null;
        try {
            String key = "large-stream-upload";
            long size = 512L * 1024 * 1024 + 10; // 512MB + 10 bytes
            InputStream stream = new RandomInputStream(size);
            SyncObject object = new SyncObject(source, key, new ObjectMetadata().withContentLength(size), stream, null);

            AwsS3Config s3Config = new AwsS3Config();
            if (endpointUri.getScheme() != null)
                s3Config.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
            s3Config.setHost(endpointUri.getHost());
            s3Config.setPort(endpointUri.getPort());
            s3Config.setAccessKey(accessKey);
            s3Config.setSecretKey(secretKey);
            s3Config.setRegion(region);
            s3Config.setLegacySignatures(true);
            s3Config.setDisableVHosts(true);
            s3Config.setBucketName(bucketName);

            s3Target = new AwsS3Storage();
            s3Target.withConfig(s3Config).withOptions(new SyncOptions());
            s3Target.configure(source, null, s3Target);

            String createdKey = s3Target.createObject(object);

            Assert.assertEquals(key, createdKey);

            // hyphen denotes an MPU
            Assert.assertTrue(s3.getObjectMetadata(bucketName, key).getETag().contains("-"));

            // verify bytes read from source
            // first wait a tick so the perf counter has at least one interval
            Thread.sleep(1000);
            Assert.assertEquals(size, object.getBytesRead());
            Assert.assertTrue(source.getReadRate() > 0);

            // should upload in series (single thread).. there's no way the stream can be split, so just verifying the MD5
            // should be sufficient. need to read the entire object since we can't use the ETag
            InputStream objectStream = s3.getObject(bucketName, key).getObjectContent();
            ChecksummedInputStream md5Stream = new ChecksummedInputStream(objectStream, new RunningChecksum(ChecksumAlgorithm.MD5));
            byte[] buffer = new byte[128 * 1024];
            int c;
            do {
                c = md5Stream.read(buffer);
            } while (c >= 0);
            md5Stream.close();

            Assert.assertEquals(object.getMd5Hex(true).toLowerCase(), md5Stream.getChecksum().getHexValue().toLowerCase());
        } finally {
            source.close();
            if (s3Target != null) s3Target.close();
            deleteObjects(bucketName);
            s3.deleteBucket(bucketName);
        }
    }

    @Ignore // only perform this test on a co-located S3 store!
    @Test
    public void testVeryLargeUploadFile() throws Exception {
        String bucketName = "ecs-sync-s3-target-test-bucket";
        createBucket(bucketName, false);

        TestStorage source = new TestStorage();
        source.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).withOptions(new SyncOptions());
        AwsS3Storage s3Target = null;
        try {
            String key = "large-file-upload";
            long size = 512L * 1024 * 1024 + 10; // 512MB + 10 bytes
            InputStream stream = new RandomInputStream(size);

            // create temp file
            File tempFile = File.createTempFile(key, null);
            tempFile.deleteOnExit();
            StreamUtil.copy(stream, new FileOutputStream(tempFile), size);

            SyncObject object = new SyncObject(source, key, new ObjectMetadata().withContentLength(size), new FileInputStream(tempFile), null);
            object.setProperty(AbstractFilesystemStorage.PROP_FILE, tempFile);

            AwsS3Config s3Config = new AwsS3Config();
            if (endpointUri.getScheme() != null)
                s3Config.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
            s3Config.setHost(endpointUri.getHost());
            s3Config.setPort(endpointUri.getPort());
            s3Config.setAccessKey(accessKey);
            s3Config.setSecretKey(secretKey);
            s3Config.setRegion(region);
            s3Config.setLegacySignatures(true);
            s3Config.setDisableVHosts(true);
            s3Config.setBucketName(bucketName);

            s3Target = new AwsS3Storage();
            s3Target.withConfig(s3Config).withOptions(new SyncOptions());
            s3Target.configure(source, null, s3Target);

            String createdKey = s3Target.createObject(object);

            Assert.assertEquals(key, createdKey);

            // hyphen denotes an MPU
            Assert.assertTrue(s3.getObjectMetadata(bucketName, key).getETag().contains("-"));

            // verify bytes read from source
            // first wait a tick so the perf counter has at least one interval
            Thread.sleep(1000);
            Assert.assertEquals(size, object.getBytesRead());
            Assert.assertTrue(source.getReadRate() > 0);

            // need to read the entire object since we can't use the ETag
            InputStream objectStream = s3.getObject(bucketName, key).getObjectContent();
            ChecksummedInputStream md5Stream = new ChecksummedInputStream(objectStream, new RunningChecksum(ChecksumAlgorithm.MD5));
            byte[] buffer = new byte[128 * 1024];
            int c;
            do {
                c = md5Stream.read(buffer);
            } while (c >= 0);
            md5Stream.close();

            Assert.assertEquals(object.getMd5Hex(true).toLowerCase(), md5Stream.getChecksum().getHexValue().toLowerCase());
        } finally {
            source.close();
            if (s3Target != null) s3Target.close();
            deleteObjects(bucketName);
            s3.deleteBucket(bucketName);
        }
    }

    private void runSync(SyncConfig syncConfig, SyncStorage source) {
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(source);

        sync.run();

        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
    }

    private void alterContent(TestStorage storage, String identifier, String version) throws Exception {
        Collection<TestStorage.TestSyncObject> children = identifier == null ? storage.getRootObjects() : storage.getChildren(identifier);
        for (TestStorage.TestSyncObject object : children) {
            if (object.getData() != null && object.getData().length > 0) object.getData()[0] += 1;
            object.getMetadata().setUserMetadataValue("version", version);
            object.getMetadata().setModificationTime(new Date());
            if (object.getMetadata().isDirectory()) {
                alterContent(storage, storage.getIdentifier(object.getRelativePath(), true), version);
            }
        }
    }

    private void verifyBuckets(String bucket1, String bucket2) {
        Map<String, SortedSet<S3VersionSummary>> map1 = getAllVersions(bucket1);
        Map<String, SortedSet<S3VersionSummary>> map2 = getAllVersions(bucket2);

        // should be sufficient to compare ETags since they should be valid
        Assert.assertEquals(map1.keySet(), map2.keySet());
        for (String key : map1.keySet()) {
            Assert.assertEquals(map1.get(key).size(), map2.get(key).size());
            Iterator<S3VersionSummary> summaries2 = map2.get(key).iterator();
            String lastEtag = null;
            for (S3VersionSummary summary1 : map1.get(key)) {
                S3VersionSummary summary2 = summaries2.next();
                Assert.assertEquals(summary1.isDeleteMarker(), summary2.isDeleteMarker());
                // probably sufficient to compare ETags
                Assert.assertEquals(summary1.getETag(), summary2.getETag());
                // also make sure it's different from the last ETag
                if (summary1.getSize() > 0) Assert.assertNotEquals(lastEtag, summary1.getETag());
                lastEtag = summary1.getETag();
            }
        }
    }

    private void verifyAcls(AccessControlList acl1, AccessControlList acl2) {
        Assert.assertEquals(acl1.getOwner(), acl2.getOwner());
        for (Grant grant : acl1.getGrantsAsList()) {
            Assert.assertTrue(acl2.getGrantsAsList().contains(grant));
        }
        for (Grant grant : acl2.getGrantsAsList()) {
            Assert.assertTrue(acl1.getGrantsAsList().contains(grant));
        }
    }

    private List<S3VersionSummary> getVersions(String bucket, String key) {
        List<S3VersionSummary> summaries = new ArrayList<>();

        VersionListing listing = null;
        do {
            if (listing == null) listing = s3.listVersions(bucket, key);
            else listing = s3.listNextBatchOfVersions(listing);

            for (S3VersionSummary summary : listing.getVersionSummaries()) {
                if (summary.getKey().equals(key)) summaries.add(summary);
            }
        } while (listing.isTruncated());

        Collections.sort(summaries, new VersionComparator());

        return summaries;
    }

    private Map<String, SortedSet<S3VersionSummary>> getAllVersions(String bucket) {
        // build comprehensive key -> versions map
        // must sort by keys and mtime for simple comparison
        Map<String, SortedSet<S3VersionSummary>> map = new TreeMap<>();

        VersionListing listing = null;
        do {
            if (listing == null) listing = s3.listVersions(bucket, null);
            else listing = s3.listNextBatchOfVersions(listing);

            for (S3VersionSummary summary : listing.getVersionSummaries()) {
                SortedSet<S3VersionSummary> versions = map.get(summary.getKey());
                if (versions == null) {
                    versions = new TreeSet<>(new VersionComparator());
                    map.put(summary.getKey(), versions);
                }
                versions.add(summary);
            }
        } while (listing.isTruncated());

        return map;
    }

    private void deleteObjects(final String bucket) {
        ExecutorService executor = Executors.newFixedThreadPool(32);
        List<Future> futures = new ArrayList<>();
        try {
            ObjectListing listing = null;
            do {
                if (listing == null) listing = s3.listObjects(bucket);
                else listing = s3.listNextBatchOfObjects(listing);

                for (final S3ObjectSummary summary : listing.getObjectSummaries()) {
                    futures.add(executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            s3.deleteObject(bucket, summary.getKey());
                        }
                    }));
                }
            } while (listing.isTruncated());

            for (Future future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    if (e instanceof RuntimeException) throw (RuntimeException) e;
                    else throw new RuntimeException(e);
                }
            }
        } finally {
            executor.shutdown();
        }
    }

    private void deleteVersionedBucket(final String bucket) {
        ExecutorService executor = Executors.newFixedThreadPool(32);
        List<Future> futures = new ArrayList<>();
        try {
            VersionListing listing = null;
            do {
                if (listing == null) listing = s3.listVersions(bucket, null);
                else listing = s3.listNextBatchOfVersions(listing);

                for (final S3VersionSummary summary : listing.getVersionSummaries()) {
                    futures.add(executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            s3.deleteVersion(bucket, summary.getKey(), summary.getVersionId());
                        }
                    }));
                }
            } while (listing.isTruncated());

            for (Future future : futures) {
                try {
                    future.get();
                } catch (Throwable t) {
                    log.warn("error deleting version", t);
                }
            }

            s3.deleteBucket(bucket);
        } catch (RuntimeException e) {
            log.warn("could not delete bucket " + bucket, e);
        } finally {
            executor.shutdown();
        }
    }

    private void createBucket(String bucket, boolean withVersioning) {
        try {
            s3.createBucket(bucket);
        } catch (AmazonServiceException e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        if (withVersioning) {
            s3.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucket,
                    new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
        }
    }

    private class VersionComparator implements Comparator<S3VersionSummary> {
        @Override
        public int compare(S3VersionSummary o1, S3VersionSummary o2) {
            int result = o1.getLastModified().compareTo(o2.getLastModified());
            if (result == 0) result = o1.getVersionId().compareTo(o2.getVersionId());
            return result;
        }
    }
}

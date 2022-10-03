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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.conn.ssl.SdkTLSSocketFactory;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.AwsS3Config;
import com.emc.ecs.sync.config.storage.S3ConfigurationException;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.file.AbstractFilesystemStorage;
import com.emc.ecs.sync.storage.s3.AbstractS3Storage;
import com.emc.ecs.sync.storage.s3.AbstractS3Test;
import com.emc.ecs.sync.storage.s3.AwsS3Storage;
import com.emc.ecs.sync.test.DelayFilter;
import com.emc.ecs.sync.test.StartNotifyFilter;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.test.TestUtil;
import com.emc.ecs.sync.util.RandomInputStream;
import com.emc.ecs.sync.util.SSLUtil;
import com.emc.object.util.ChecksumAlgorithm;
import com.emc.object.util.ChecksummedInputStream;
import com.emc.object.util.RunningChecksum;
import com.emc.rest.util.StreamUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AwsS3Test extends AbstractS3Test {
    private static final Logger log = LoggerFactory.getLogger(AwsS3Test.class);

    @Override
    protected AwsS3Storage createStorageInstance() {
        return (AwsS3Storage) new AwsS3Storage().withConfig(generateConfig());
    }

    @Override
    protected AwsS3Config generateConfig() {
        return generateConfig(getTestBucket());
    }

    @Override
    protected String getTestBucket() {
        return "ecs-sync-aws-s3-test-bucket";
    }

    AwsS3Config generateConfig(String bucket) {
        URI endpointUri = URI.create(getS3Endpoint());
        AwsS3Config s3Config = new AwsS3Config();
        if (endpointUri.getScheme() != null)
            s3Config.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
        s3Config.setHost(endpointUri.getHost());
        s3Config.setPort(endpointUri.getPort());
        s3Config.setAccessKey(getS3AccessKey());
        s3Config.setSecretKey(getS3SecretKey());
        s3Config.setRegion(getS3Region());
        s3Config.setDisableVHosts(true);
        s3Config.setBucketName(bucket);
        return s3Config;
    }

    @Test
    public void testVersions() {
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

            AwsS3Config s3Config1 = generateConfig(bucket1);

            SyncConfig syncConfig = new SyncConfig().withOptions(options);
            syncConfig.setTarget(s3Config1);

            // push it into bucket1
            log.info("writing v1 source data...");
            TestStorage testSource = new TestStorage();
            testSource.withConfig(testConfig).withOptions(options);
            runSync(syncConfig, testSource);

            // 2nd version is delete
            log.info("deleting source objects (for v2)...");
            deleteObjectsInBucket(bucket1);

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

            AwsS3Config s3Config2 = generateConfig(bucket2);
            new AwsS3Config();
            s3Config2.setCreateBucket(true);
            s3Config2.setIncludeVersions(true);

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
            deleteObjectsInBucket(bucket1);

            // test deleted objects
            log.info("migrating v5 to target...");
            options.withVerifyOnly(false).withVerify(true);
            runSync(syncConfig, null);
            log.info("verifying versions...");
            verifyBuckets(bucket1, bucket2);

            // test deleted objects from scratch
            log.info("wiping bucket2...");
            deleteBucket(bucket2);
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
            deleteBucket(bucket1);
            log.info("cleaning up bucket2...");
            deleteBucket(bucket2);
        }
    }

    @Test
    public void testSetAcl() {
        String key = "test-object";
        enableVersioning(getTestBucket());
        Owner owner = amazonS3.getBucketAcl(getTestBucket()).getOwner();

        String content = "hello ACLs";

        amazonS3.putObject(getTestBucket(), key, new ByteArrayInputStream(content.getBytes()), null); // 1st version

        AccessControlList acl = new AccessControlList();
        acl.setOwner(owner);
        acl.grantPermission(new CanonicalGrantee(owner.getId()), Permission.FullControl);
        acl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Read);
        acl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Write);
        acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);

        PutObjectRequest putRequest = new PutObjectRequest(getTestBucket(), key, new ByteArrayInputStream(content.getBytes()), null);
        putRequest.setAccessControlList(acl);

        amazonS3.putObject(putRequest); // 2nd version

        AccessControlList remoteAcl = amazonS3.getObjectAcl(getTestBucket(), key);

        verifyAcls(acl, remoteAcl);
    }

    @Test
    public void testSyncVersionsWithAcls() {
        String bucket1 = "ecs-sync-s3-test-sync-acl1";
        String bucket2 = "ecs-sync-s3-test-sync-acl2";
        createBucket(bucket1, true);
        Owner owner = amazonS3.getBucketAcl(bucket1).getOwner();

        String key1 = "key1", key2 = "key2", key3 = "key3";

        AccessControlList largeAcl = new AccessControlList();
        largeAcl.setOwner(owner);
        largeAcl.grantPermission(new CanonicalGrantee(owner.getId()), Permission.FullControl);
        largeAcl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Read);
        largeAcl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Write);
        largeAcl.grantPermission(GroupGrantee.AllUsers, Permission.Read);

        AccessControlList midAcl = new AccessControlList();
        midAcl.setOwner(owner);
        midAcl.grantPermission(new CanonicalGrantee(owner.getId()), Permission.FullControl);
        midAcl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Read);

        AccessControlList defaultAcl = new AccessControlList();
        defaultAcl.setOwner(owner);
        defaultAcl.grantPermission(new CanonicalGrantee(owner.getId()), Permission.FullControl);

        try {
            // default acls
            amazonS3.putObject(bucket1, key1, new ByteArrayInputStream("data1".getBytes()), null);
            amazonS3.putObject(bucket1, key1, new ByteArrayInputStream("data1".getBytes()), null);
            amazonS3.putObject(bucket1, key1, new ByteArrayInputStream("data1".getBytes()), null);

            // default acl on latest
            PutObjectRequest request = new PutObjectRequest(bucket1, key2, new ByteArrayInputStream("data2".getBytes()), null);
            request.setAccessControlList(largeAcl);
            amazonS3.putObject(request);
            request = new PutObjectRequest(bucket1, key2, new ByteArrayInputStream("data2".getBytes()), null);
            request.setAccessControlList(midAcl);
            amazonS3.putObject(request);
            amazonS3.putObject(bucket1, key2, new ByteArrayInputStream("data2".getBytes()), null);

            // default acl on first version
            amazonS3.putObject(bucket1, key3, new ByteArrayInputStream("data3".getBytes()), null);
            request = new PutObjectRequest(bucket1, key3, new ByteArrayInputStream("data3".getBytes()), null);
            request.setAccessControlList(midAcl);
            amazonS3.putObject(request);
            request = new PutObjectRequest(bucket1, key3, new ByteArrayInputStream("data3".getBytes()), null);
            request.setAccessControlList(largeAcl);
            amazonS3.putObject(request);

            AwsS3Config sourceConfig = generateConfig(bucket1);
            sourceConfig.setIncludeVersions(true);

            AwsS3Config targetConfig = generateConfig(bucket2);
            new AwsS3Config();
            targetConfig.setCreateBucket(true);
            targetConfig.setIncludeVersions(true);

            SyncOptions options = new SyncOptions().withThreadCount(1).withVerify(true).withSyncAcl(true);

            SyncConfig syncConfig = new SyncConfig().withOptions(options).withSource(sourceConfig).withTarget(targetConfig);

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            TestUtil.run(sync);

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

            List<S3VersionSummary> key1Versions = getVersions(bucket2, key1);
            verifyAcls(defaultAcl, amazonS3.getObjectAcl(bucket2, key1, key1Versions.get(0).getVersionId()));
            verifyAcls(defaultAcl, amazonS3.getObjectAcl(bucket2, key1, key1Versions.get(1).getVersionId()));
            verifyAcls(defaultAcl, amazonS3.getObjectAcl(bucket2, key1, key1Versions.get(2).getVersionId()));

            List<S3VersionSummary> key2Versions = getVersions(bucket2, key2);
            verifyAcls(largeAcl, amazonS3.getObjectAcl(bucket2, key2, key2Versions.get(0).getVersionId()));
            verifyAcls(midAcl, amazonS3.getObjectAcl(bucket2, key2, key2Versions.get(1).getVersionId()));
            verifyAcls(defaultAcl, amazonS3.getObjectAcl(bucket2, key2, key2Versions.get(2).getVersionId()));

            List<S3VersionSummary> key3Versions = getVersions(bucket2, key3);
            verifyAcls(defaultAcl, amazonS3.getObjectAcl(bucket2, key3, key3Versions.get(0).getVersionId()));
            verifyAcls(midAcl, amazonS3.getObjectAcl(bucket2, key3, key3Versions.get(1).getVersionId()));
            verifyAcls(largeAcl, amazonS3.getObjectAcl(bucket2, key3, key3Versions.get(2).getVersionId()));
        } finally {
            deleteBucket(bucket1);
            deleteBucket(bucket2);
        }
    }

    @Test
    public void testNormalUpload() throws Exception {
        try (TestStorage source = new TestStorage();
             AwsS3Storage s3Target = new AwsS3Storage()) {
            source.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).withOptions(new SyncOptions());

            String key = "normal-upload";
            long size = 512 * 1024; // 512KiB
            InputStream stream = new RandomInputStream(size);
            SyncObject object = new SyncObject(source, key, new ObjectMetadata().withContentLength(size), stream, null);

            AwsS3Config s3Config = generateConfig(getTestBucket());

            s3Target.withConfig(s3Config).withOptions(new SyncOptions());
            s3Target.configure(source, null, s3Target);

            String createdKey = s3Target.createObject(object);

            Assertions.assertEquals(key, createdKey);

            // verify bytes read from source
            // first wait a tick so the perf counter has at least one interval
            Thread.sleep(1000);
            Assertions.assertEquals(size, object.getBytesRead());
            Assertions.assertTrue(source.getReadRate() > 0);

            // proper ETag means no MPU was performed
            Assertions.assertEquals(object.getMd5Hex(true).toLowerCase(), amazonS3.getObjectMetadata(getTestBucket(), key).getETag().toLowerCase());
        }
    }

    @Test
    public void testNormalUploadSTS() throws Exception {
        // must have an STS endpoint
        Assumptions.assumeTrue(getStsEndpoint() != null);

        try (TestStorage source = new TestStorage();
             AwsS3Storage s3Target = new AwsS3Storage()) {
            source.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).withOptions(new SyncOptions());

            String key = "normal-upload";
            long size = 512 * 1024; // 512KiB
            InputStream stream = new RandomInputStream(size);
            SyncObject object = new SyncObject(source, key, new ObjectMetadata().withContentLength(size), stream, null);

            AwsS3Config s3Config = generateConfig(getTestBucket());

            final AWSSecurityTokenService awsSecurityTokenService = AWSSecurityTokenServiceClientBuilder.standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(getStsEndpoint(), getS3Region()))
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(getS3AccessKey(), getS3SecretKey())))
                    .build();
            GetSessionTokenRequest sessionTokenRequest = new GetSessionTokenRequest().withDurationSeconds(7200);
            final Credentials stsCredentials = awsSecurityTokenService.getSessionToken(sessionTokenRequest).getCredentials();

            s3Config.setAccessKey(stsCredentials.getAccessKeyId());
            s3Config.setSecretKey(stsCredentials.getSecretAccessKey());
            s3Config.setSessionToken(stsCredentials.getSessionToken());

            s3Target.withConfig(s3Config).withOptions(new SyncOptions());
            s3Target.configure(source, null, s3Target);

            String createdKey = s3Target.createObject(object);

            Assertions.assertEquals(key, createdKey);

            // verify bytes read from source
            // first wait a tick so the perf counter has at least one interval
            Thread.sleep(1000);
            Assertions.assertEquals(size, object.getBytesRead());
            Assertions.assertTrue(source.getReadRate() > 0);

            // proper ETag means no MPU was performed
            Assertions.assertEquals(object.getMd5Hex(true).toLowerCase(), amazonS3.getObjectMetadata(getTestBucket(), key).getETag().toLowerCase());
        }
    }

    @Test
    public void testVeryLargeUploadStream() throws Exception {
        // only enable this when testing against a local lab cluster
        Assumptions.assumeTrue(Boolean.parseBoolean(TestConfig.getProperties().getProperty(TestConfig.PROP_LARGE_DATA_TESTS)));

        try (TestStorage source = new TestStorage();
             AwsS3Storage s3Target = new AwsS3Storage()) {
            source.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).withOptions(new SyncOptions());

            String key = "large-stream-upload";
            long size = 512L * 1024 * 1024 + 10; // 512MB + 10 bytes
            InputStream stream = new RandomInputStream(size);
            SyncObject object = new SyncObject(source, key, new ObjectMetadata().withContentLength(size), stream, null);

            AwsS3Config s3Config = generateConfig(getTestBucket());

            s3Target.withConfig(s3Config).withOptions(new SyncOptions());
            s3Target.configure(source, null, s3Target);

            String createdKey = s3Target.createObject(object);

            Assertions.assertEquals(key, createdKey);

            // hyphen denotes an MPU
            Assertions.assertTrue(amazonS3.getObjectMetadata(getTestBucket(), key).getETag().contains("-"));

            // verify bytes read from source
            // first wait a tick so the perf counter has at least one interval
            Thread.sleep(1000);
            Assertions.assertEquals(size, object.getBytesRead());
            Assertions.assertTrue(source.getReadRate() > 0);

            // should upload in series (single thread).. there's no way the stream can be split, so just verifying the MD5
            // should be sufficient. need to read the entire object since we can't use the ETag
            InputStream objectStream = amazonS3.getObject(getTestBucket(), key).getObjectContent();
            ChecksummedInputStream md5Stream = new ChecksummedInputStream(objectStream, new RunningChecksum(ChecksumAlgorithm.MD5));
            byte[] buffer = new byte[128 * 1024];
            int c;
            do {
                c = md5Stream.read(buffer);
            } while (c >= 0);
            md5Stream.close();

            Assertions.assertEquals(object.getMd5Hex(true).toLowerCase(), md5Stream.getChecksum().getHexValue().toLowerCase());
        }
    }

    @Test
    public void testVeryLargeUploadFile() throws Exception {
        // only enable this when testing against a local lab cluster
        Assumptions.assumeTrue(Boolean.parseBoolean(TestConfig.getProperties().getProperty(TestConfig.PROP_LARGE_DATA_TESTS)));

        try (TestStorage source = new TestStorage();
             AwsS3Storage s3Target = new AwsS3Storage()) {
            source.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).withOptions(new SyncOptions());

            String key = "large-file-upload";
            long size = 512L * 1024 * 1024 + 10; // 512MB + 10 bytes
            InputStream stream = new RandomInputStream(size);

            // create temp file
            File tempFile = File.createTempFile(key, null);
            tempFile.deleteOnExit();
            StreamUtil.copy(stream, Files.newOutputStream(tempFile.toPath()), size);

            SyncObject object = new SyncObject(source, key, new ObjectMetadata().withContentLength(size), Files.newInputStream(tempFile.toPath()), null);
            object.setProperty(AbstractFilesystemStorage.PROP_FILE, tempFile);

            AwsS3Config s3Config = generateConfig(getTestBucket());

            s3Target.withConfig(s3Config).withOptions(new SyncOptions());
            s3Target.configure(source, null, s3Target);

            String createdKey = s3Target.createObject(object);

            Assertions.assertEquals(key, createdKey);

            // hyphen denotes an MPU
            Assertions.assertTrue(amazonS3.getObjectMetadata(getTestBucket(), key).getETag().contains("-"));

            // verify bytes read from source
            // first wait a tick so the perf counter has at least one interval
            Thread.sleep(1000);
            Assertions.assertEquals(size, object.getBytesRead());
            Assertions.assertTrue(source.getReadRate() > 0);

            // need to read the entire object since we can't use the ETag
            InputStream objectStream = amazonS3.getObject(getTestBucket(), key).getObjectContent();
            ChecksummedInputStream md5Stream = new ChecksummedInputStream(objectStream, new RunningChecksum(ChecksumAlgorithm.MD5));
            byte[] buffer = new byte[128 * 1024];
            int c;
            do {
                c = md5Stream.read(buffer);
            } while (c >= 0);
            md5Stream.close();

            Assertions.assertEquals(object.getMd5Hex(true).toLowerCase(), md5Stream.getChecksum().getHexValue().toLowerCase());
        }
    }

    @Test
    public void testServerSideEncryption() {
        try (TestStorage source = new TestStorage();
             AwsS3Storage s3Target = new AwsS3Storage()) {
            source.withConfig(new com.emc.ecs.sync.config.storage.TestConfig()).withOptions(new SyncOptions());

            String key = "SSE-S3-upload";
            SyncObject object = new SyncObject(source, key, new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), null);

            AwsS3Config s3Config = generateConfig(getTestBucket());
            s3Config.setSseS3Enabled(true);

            s3Target.withConfig(s3Config).withOptions(new SyncOptions());
            s3Target.configure(source, null, s3Target);

            String createdKey = s3Target.createObject(object);
            Assertions.assertEquals(key, createdKey);
            Assertions.assertEquals(object.getMd5Hex(true).toLowerCase(), amazonS3.getObjectMetadata(getTestBucket(), key).getETag().toLowerCase());
            String sseAlgorithm = amazonS3.getObjectMetadata(getTestBucket(), key).getSSEAlgorithm();
            Assertions.assertEquals(com.amazonaws.services.s3.model.ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION, sseAlgorithm);
        }
    }

    @Test
    public void testTrustPrivateTLSCert() throws Exception {
        URI endpointUri = URI.create(getS3Endpoint());
        // retrieve the corresponding https port from endpointUri
        int port = 443;
        if (endpointUri.getScheme().equals(Protocol.http.toString())) {
            if (endpointUri.getPort() == 9020) {
                port = 9021;
            } else if (endpointUri.getPort() != -1 && endpointUri.getPort() != 80) {
                Assumptions.assumeFalse(true, "None default customised HTTP port is skipped for test");
            }
        } else {
            port = endpointUri.getPort() != -1 ? endpointUri.getPort() : 443;
        }
        String endpoint = "https://" + endpointUri.getHost() + ":" + port;

        ClientConfiguration config = new ClientConfiguration().withSignerOverride("S3SignerType");
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(getS3AccessKey(), getS3SecretKey())))
                .withClientConfiguration(config)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, getS3Region()));
        try {
            amazonS3 = builder.build();
            amazonS3.doesBucketExistV2("testTrustPrivateTLSCert");
            Assumptions.assumeFalse(true, "The test requires target server with untrusted TLS certificate");
        } catch (Exception e) {
            if (!(e instanceof SdkClientException && e.getMessage().contains("PKIX path building failed")))
                throw e;
        }

        X509Certificate[] certChain = getTLSCertChain(endpointUri.getHost(), port);
        Assertions.assertNotNull(certChain);
        Assertions.assertTrue(certChain.length > 0);

        String base64EncodedCert = Base64.getEncoder().encodeToString(certChain[0].getEncoded());
        SSLContext sslContext = SSLUtil.getSSLContext(base64EncodedCert);
        SdkTLSSocketFactory sslSocketFactory = new SdkTLSSocketFactory(sslContext, null);
        config.getApacheHttpClientConfig().setSslSocketFactory(sslSocketFactory);
        builder.setClientConfiguration(config);
        amazonS3 = builder.build();
        amazonS3.doesBucketExistV2("testTrustPrivateTLSCert");
    }

    @Test
    public void testSyncWithBase64TlsCertificate() throws Exception {
        URI endpointUri = URI.create(getS3Endpoint());
        String key = "testSyncWithBase64TlsCertificate";
        String base64TlsCertificate = Base64.getEncoder().encodeToString("Fake Certificate".getBytes(StandardCharsets.UTF_8));
        try (TestStorage testStorage = new TestStorage();
             AwsS3Storage s3Target = new AwsS3Storage()) {
            testStorage.withConfig(new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false)).withOptions(new SyncOptions());
            testStorage.createObject(new SyncObject(testStorage, key, new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), null));

            // retrieve the corresponding https port from endpointUri
            int port = 443;
            if (endpointUri.getScheme().equals(Protocol.http.toString())) {
                if (endpointUri.getPort() == 9020) {
                    port = 9021;
                } else if (endpointUri.getPort() != -1 && endpointUri.getPort() != 80) {
                    Assumptions.assumeFalse(true, "None default customised HTTP port is skipped for test");
                }
            } else {
                port = endpointUri.getPort() != -1 ? endpointUri.getPort() : 443;
            }

            AwsS3Config s3Config = generateConfig(getTestBucket());
            s3Config.setProtocol(Protocol.https);
            s3Config.setPort(port);
            s3Config.setBase64TlsCertificate(base64TlsCertificate);
            s3Config.setCreateBucket(true);

            s3Target.withConfig(s3Config).withOptions(new SyncOptions());
            try {
                s3Target.configure(testStorage, null, s3Target);
                Assertions.fail("Expect S3ConfigurationException on non-base64 encoded certificate");
            } catch (S3ConfigurationException e) {
                Assertions.assertEquals(S3ConfigurationException.Error.ERROR_INVALID_TLS_CERTIFICATE, e.getError());
            }

            X509Certificate[] certChain = getTLSCertChain(endpointUri.getHost(), port);
            Assertions.assertNotNull(certChain);
            Assertions.assertTrue(certChain.length > 0);
            base64TlsCertificate = Base64.getEncoder().encodeToString(certChain[0].getEncoded());
            s3Config.setBase64TlsCertificate(base64TlsCertificate);

            SyncConfig syncConfig = new SyncConfig().withTarget(s3Config).withSource(testStorage.getConfig());
            EcsSync sync = new EcsSync();
            sync.setSource(testStorage);
            sync.setTarget(s3Target);
            sync.setSyncConfig(syncConfig);
            TestUtil.run(sync);

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
        }
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

        AwsS3Config targetConfig = generateConfig();
        targetConfig.setMpuResumeEnabled(enableResume);
        targetConfig.setMpuPartSizeMb((int) mockMultipartSource.getPartSize() / 1024 / 1024);
        targetConfig.setMpuThresholdMb(targetConfig.getMpuPartSizeMb());

        try (TestStorage testStorage = new TestStorage();
             AwsS3Storage targetStorage = new AwsS3Storage()) {
            targetStorage.withConfig(targetConfig);
            testStorage.withConfig(new com.emc.ecs.sync.config.storage.TestConfig());

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
            sync.setTarget(targetStorage);

            // ingest the object into the source first
            // NOTE: test objects are not multi-part sources, so MPUs will be sequential, not parallel
            testStorage.getConfig().withDiscardData(false);
            testStorage.createObject(new SyncObject(testStorage, key,
                    new com.emc.ecs.sync.model.ObjectMetadata().withContentLength(mockMultipartSource.getTotalSize())
                            .withContentType("text/plain"),
                    mockMultipartSource.getCompleteDataStream(), null));

            CompletableFuture<?> future = CompletableFuture.runAsync(sync);
            startNotifyConfig.waitForStart();
            Thread.sleep(delayMs); // need to terminate after the first part is started, but before it finishes
            sync.terminate();
            future.get();

            if (enableResume) {
                // object should not exist
                Assertions.assertThrows(AmazonS3Exception.class, () -> amazonS3.getObjectMetadata(getTestBucket(), key));
                // MPU should exist
                MultipartUploadListing multipartUploadListing = amazonS3.listMultipartUploads(new ListMultipartUploadsRequest(getTestBucket()).withPrefix(key));
                Assertions.assertEquals(1, multipartUploadListing.getMultipartUploads().size());
                // MPU should have only 1 part
                String uploadId = multipartUploadListing.getMultipartUploads().get(0).getUploadId();
                Assertions.assertEquals(1, amazonS3.listParts(new ListPartsRequest(getTestBucket(), key, uploadId)).getParts().size());

                EcsSync sync2 = new EcsSync();
                sync2.setSyncConfig(syncConfig);
                sync2.setSource(testStorage);
                sync2.setTarget(targetStorage);

                TestUtil.run(sync2);
                Assertions.assertEquals(1, sync2.getStats().getObjectsComplete());
            } else {
                Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
            }

            Assertions.assertEquals(0, amazonS3.listMultipartUploads(new ListMultipartUploadsRequest(getTestBucket()).withPrefix(key)).getMultipartUploads().size());
            com.amazonaws.services.s3.model.ObjectMetadata metadata = amazonS3.getObjectMetadata(getTestBucket(), key);
            Assertions.assertEquals(mockMultipartSource.getMpuETag(), metadata.getETag());
            Assertions.assertEquals(mockMultipartSource.getTotalSize(), metadata.getContentLength());
        }
    }

    // AWS SDK uses a connection pool, which is limited (by AwsS3Storage) to 500 connections
    // this tests multiple jobs, each using 100 threads (and connection pool size), but together using 1000 connections,
    // to make sure multiple S3 clients don't share connection pools
    @Test
    public void testSeparateJobConnectionPools() throws Exception {
        // only enable this when testing against a local lab cluster
        Assumptions.assumeTrue(Boolean.parseBoolean(TestConfig.getProperties().getProperty(TestConfig.PROP_LARGE_DATA_TESTS)));

        // 1000 1MB objects total
        int objectSize = 1024 * 1024, objectCount = 100, threadCount = 100, jobCount = 10;

        // jobs must have separate instances of SyncConfig (they do not clone them), so we can't simply tweak a single
        // instance and pass it to a different job
        ConfigGenerator<String> targetConfigGenerator = prefix -> {
            AwsS3Config targetConfig = generateConfig();
            targetConfig.setKeyPrefix(prefix);

            return new SyncConfig()
                    .withSource(new com.emc.ecs.sync.config.storage.TestConfig()
                            .withObjectCount(objectCount).withMinSize(objectSize).withMaxSize(objectSize)
                            .withChanceOfChildren(0))
                    .withTarget(targetConfig)
                    .withOptions(new SyncOptions().withThreadCount(threadCount));
        };
        ConfigGenerator<String> sourceConfigGenerator = prefix -> {
            AwsS3Config sourceConfig = generateConfig();
            sourceConfig.setKeyPrefix(prefix);

            return new SyncConfig()
                    .withSource(sourceConfig)
                    .withTarget(new com.emc.ecs.sync.config.storage.TestConfig())
                    .withOptions(new SyncOptions().withThreadCount(threadCount));
        };

        // fire up all jobs
        ExecutorService jobPool = Executors.newFixedThreadPool(jobCount);
        try {
            // first test as a target
            List<Future<EcsSync>> futures = new ArrayList<>();
            for (int i = 0; i < jobCount; i++) {
                EcsSync syncJob = new EcsSync();
                // to make cleanup easier, we will target the same bucket, so each job needs to use a different
                // prefix, otherwise the keys will clash
                syncJob.setSyncConfig(targetConfigGenerator.generateConfig("job" + i + "/"));
                futures.add(jobPool.submit(() -> {
                    TestUtil.run(syncJob);
                    return syncJob;
                }));
            }

            for (Future<EcsSync> future : futures) {
                EcsSync syncJob = future.get();
                Assertions.assertEquals(0, syncJob.getStats().getObjectsFailed());
                Assertions.assertEquals(objectCount, syncJob.getStats().getObjectsComplete());
            }

            // then test as a source
            futures.clear();
            for (int i = 0; i < jobCount; i++) {
                EcsSync syncJob = new EcsSync();
                // to make cleanup easier, we will target the same bucket, so each job needs to use a different
                // prefix, otherwise the keys will clash
                syncJob.setSyncConfig(sourceConfigGenerator.generateConfig("job" + i + "/"));
                futures.add(jobPool.submit(() -> {
                    TestUtil.run(syncJob);
                    return syncJob;
                }));
            }
            jobPool.shutdown();

            for (Future<EcsSync> future : futures) {
                EcsSync syncJob = future.get();
                Assertions.assertEquals(0, syncJob.getStats().getObjectsFailed());
                Assertions.assertEquals(objectCount, syncJob.getStats().getObjectsComplete());
            }
        } finally {
            if (!jobPool.awaitTermination(1, TimeUnit.MINUTES))
                log.warn("job pool never terminated");
        }
    }

    @Test
    public void testSkipBehavior() throws Exception {
        // only enable this when testing against a local lab cluster
        Assumptions.assumeTrue(Boolean.parseBoolean(TestConfig.getProperties().getProperty(TestConfig.PROP_LARGE_DATA_TESTS)));

        int smObjectSize = 10_240; // 10K
        int lgObjectSize = 100 * 1024 * 1024; // 100M
        int objectCount = 100;

        try (TestStorage testStorage = new TestStorage()) {
            com.emc.ecs.sync.config.storage.TestConfig testConfig = new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false);
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
            AwsS3Config targetConfig = generateConfig();
            targetConfig.setStoreSourceObjectCopyMarkers(true);
            targetConfig.setMpuThresholdMb(lgObjectSize);
            targetConfig.setMpuPartSizeMb(lgObjectSize / 8);

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
    }

    // tests if MPU copy is used when a large MPU object only has a metadata update on the source
    @Test
    public void testMpuCopy() throws Exception {
        // only enable this when testing against a local lab cluster
        Assumptions.assumeTrue(Boolean.parseBoolean(TestConfig.getProperties().getProperty(TestConfig.PROP_LARGE_DATA_TESTS)));
        String key = "mpu-copy-test", testEtag = "d41d8cd98f00b204e9800998ecf8427e";
        long objectSize = 5L * 1024 * 1024 * 1024 + 123;// just over 5 GB

        AwsS3Config s3Config = generateConfig();
        s3Config.setStoreSourceObjectCopyMarkers(true);
        try (AwsS3Storage awsS3Storage = new AwsS3Storage();
             TestStorage testStorage = new TestStorage()) {
            // write an object that will force MPU copy (must be > 5GB)
            Date mtime1 = new Date();
            awsS3Storage.withConfig(s3Config).withOptions(new SyncOptions());
            awsS3Storage.configure(testStorage, Collections.emptyIterator(), awsS3Storage);
            ObjectMetadata metadata = new ObjectMetadata().withModificationTime(mtime1).withContentLength(objectSize);
            metadata.setHttpEtag(testEtag); // explicitly set ETag for testing (this is not the real ETag of the data)
            SyncObject testObject = testStorage.new TestSyncObject(testStorage, key, metadata);
            awsS3Storage.updateObject(key, testObject);

            // should have written the object data
            Assertions.assertEquals(objectSize, testObject.getBytesRead());
            // make sure copy marker metadata is set
            com.amazonaws.services.s3.model.ObjectMetadata s3Metadata = amazonS3.getObjectMetadata(getTestBucket(), key);
            Assertions.assertEquals(String.valueOf(mtime1.getTime()), s3Metadata.getUserMetaDataOf(AbstractS3Storage.UMD_KEY_SOURCE_MTIME));
            Assertions.assertEquals(testEtag, s3Metadata.getUserMetaDataOf(AbstractS3Storage.UMD_KEY_SOURCE_ETAG));

            // now write (the same) object again, with updated mtime and same etag
            Date mtime2 = new Date();
            metadata = new ObjectMetadata().withModificationTime(mtime2).withContentLength(objectSize);
            metadata.setHttpEtag(testEtag); // explicitly set ETag for testing (this is not the real ETag of the data)
            testObject = testStorage.new TestSyncObject(testStorage, key, metadata);
            // beforeUpdate() flags the object as a metadata update only - and it is called by the TargetFilter,
            // but since we are bypassing that in this test, we must call it explicitly
            awsS3Storage.beforeUpdate(new ObjectContext().withObject(testObject), awsS3Storage.loadObject(key));
            awsS3Storage.updateObject(key, testObject);

            // make sure no data was transferred
            Assertions.assertEquals(0, testObject.getBytesRead());
            // but object metadata should be updated
            s3Metadata = amazonS3.getObjectMetadata(getTestBucket(), key);
            // new source-mtime
            Assertions.assertEquals(String.valueOf(mtime2.getTime()), s3Metadata.getUserMetaDataOf(AbstractS3Storage.UMD_KEY_SOURCE_MTIME));
            // but etag should be the same
            Assertions.assertEquals(testEtag, s3Metadata.getUserMetaDataOf(AbstractS3Storage.UMD_KEY_SOURCE_ETAG));
        }
    }

    @Test
    public void testMpuAbort() throws Exception {
        // only enable this when testing against a local lab cluster
        Assumptions.assumeTrue(Boolean.parseBoolean(TestConfig.getProperties().getProperty(TestConfig.PROP_LARGE_DATA_TESTS)));

        String key = "testMpuAbort";
        int delayMs = 1000; // needed for part upload synchronization (see below)
        AwsS3LargeFileUploaderTest.MockMultipartSource mockMultipartSource = new AwsS3LargeFileUploaderTest.MockMultipartSource();
        String exceptionMessage = "mpu-abort-exception-message";

        AwsS3Config targetConfig = generateConfig();
        targetConfig.setCreateBucket(true);
        targetConfig.setMpuPartSizeMb((int) mockMultipartSource.getPartSize() / 1024 / 1024);
        targetConfig.setMpuThresholdMb(targetConfig.getMpuPartSizeMb());

        try (TestStorage testStorage = new TestStorage()) {
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
                amazonS3.getObjectMetadata(getTestBucket(), key);
            } catch (AmazonS3Exception e) {
                Assertions.assertEquals(404, e.getStatusCode());
            }
            // MPU should exist
            MultipartUploadListing multipartUploadListing = amazonS3.listMultipartUploads(new ListMultipartUploadsRequest(getTestBucket()).withPrefix(key));
            Assertions.assertEquals(1, multipartUploadListing.getMultipartUploads().size());
            // MPU should have only 1 part
            String uploadId = multipartUploadListing.getMultipartUploads().get(0).getUploadId();
            List<PartSummary> parts = amazonS3.listParts(new ListPartsRequest(getTestBucket(), key, uploadId)).getParts();
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
                amazonS3.getObjectMetadata(getTestBucket(), key);
            } catch (AmazonS3Exception e) {
                Assertions.assertEquals(404, e.getStatusCode());
            }
            // same MPU should still exist
            multipartUploadListing = amazonS3.listMultipartUploads(new ListMultipartUploadsRequest(getTestBucket()).withPrefix(key));
            Assertions.assertEquals(1, multipartUploadListing.getMultipartUploads().size());
            // MPU should have 2 parts
            parts = amazonS3.listParts(new ListPartsRequest(getTestBucket(), key, uploadId)).getParts();
            Assertions.assertEquals(2, parts.size());
            // make sure parts are the correct size
            Assertions.assertEquals(mockMultipartSource.getPartSize(), parts.get(0).getSize());
            Assertions.assertEquals(mockMultipartSource.getPartSize(), parts.get(1).getSize());

            // clean up the MPU
            amazonS3.abortMultipartUpload(new AbortMultipartUploadRequest(getTestBucket(), key, uploadId));

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
                amazonS3.getObjectMetadata(getTestBucket(), key);
            } catch (AmazonS3Exception e) {
                Assertions.assertEquals(404, e.getStatusCode());
            }
            // MPU should not exist (should have been aborted)
            multipartUploadListing = amazonS3.listMultipartUploads(new ListMultipartUploadsRequest(getTestBucket()).withPrefix(key));
            Assertions.assertEquals(0, multipartUploadListing.getMultipartUploads().size());
        }
    }

    private void runSync(SyncConfig syncConfig, SyncStorage<?> source) {
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(source);

        TestUtil.run(sync);

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
    }

    private void alterContent(TestStorage storage, String identifier, String version) {
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
        Assertions.assertEquals(map1.keySet(), map2.keySet());
        for (String key : map1.keySet()) {
            Assertions.assertEquals(map1.get(key).size(), map2.get(key).size());
            Iterator<S3VersionSummary> summaries2 = map2.get(key).iterator();
            String lastEtag = null;
            for (S3VersionSummary summary1 : map1.get(key)) {
                S3VersionSummary summary2 = summaries2.next();
                Assertions.assertEquals(summary1.isDeleteMarker(), summary2.isDeleteMarker());
                // probably sufficient to compare ETags
                Assertions.assertEquals(summary1.getETag(), summary2.getETag());
                // also make sure it's different from the last ETag
                if (summary1.getSize() > 0) Assertions.assertNotEquals(lastEtag, summary1.getETag());
                lastEtag = summary1.getETag();
            }
        }
    }

    private void verifyAcls(AccessControlList acl1, AccessControlList acl2) {
        Assertions.assertEquals(acl1.getOwner(), acl2.getOwner());
        for (Grant grant : acl1.getGrantsAsList()) {
            Assertions.assertTrue(acl2.getGrantsAsList().contains(grant));
        }
        for (Grant grant : acl2.getGrantsAsList()) {
            Assertions.assertTrue(acl1.getGrantsAsList().contains(grant));
        }
    }

    private List<S3VersionSummary> getVersions(String bucket, String key) {
        List<S3VersionSummary> summaries = new ArrayList<>();

        VersionListing listing = null;
        do {
            if (listing == null) listing = amazonS3.listVersions(bucket, key);
            else listing = amazonS3.listNextBatchOfVersions(listing);

            for (S3VersionSummary summary : listing.getVersionSummaries()) {
                if (summary.getKey().equals(key)) summaries.add(summary);
            }
        } while (listing.isTruncated());

        summaries.sort(new VersionComparator());

        return summaries;
    }

    private Map<String, SortedSet<S3VersionSummary>> getAllVersions(String bucket) {
        // build comprehensive key -> versions map
        // must sort by keys and mtime for simple comparison
        Map<String, SortedSet<S3VersionSummary>> map = new TreeMap<>();

        VersionListing listing = null;
        do {
            if (listing == null) listing = amazonS3.listVersions(bucket, null);
            else listing = amazonS3.listNextBatchOfVersions(listing);

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

    private static class VersionComparator implements Comparator<S3VersionSummary> {
        @Override
        public int compare(S3VersionSummary o1, S3VersionSummary o2) {
            int result = o1.getLastModified().compareTo(o2.getLastModified());
            if (result == 0) result = o1.getVersionId().compareTo(o2.getVersionId());
            return result;
        }
    }

    private X509Certificate[] getTLSCertChain(String host, int port) throws SSLException {
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            CertificateTrustManager tm = new CertificateTrustManager();
            context.init(null, new TrustManager[]{tm}, null);
            SSLSocketFactory factory = context.getSocketFactory();
            try (SSLSocket socket = (SSLSocket) factory.createSocket(host, port)) {
                socket.setSoTimeout(10000);
                socket.startHandshake();
            } catch (SSLException ignored) {
                //Ignore Exception from untrusted Certificate
            }
            return tm.chain;
        } catch (Exception e) {
            throw new SSLException("Failed to get TLS Certificate Chain", e);
        }
    }

    private static class CertificateTrustManager implements X509TrustManager {
        private X509Certificate[] chain;

        CertificateTrustManager() {
        }

        public X509Certificate[] getAcceptedIssuers() {
            throw new UnsupportedOperationException();
        }

        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            throw new UnsupportedOperationException();
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            this.chain = chain;
        }
    }

    private interface ConfigGenerator<T> {
        SyncConfig generateConfig(T param);
    }
}

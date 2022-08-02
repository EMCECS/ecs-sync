/*
 * Copyright (c) 2020-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.storage.AzureBlobConfig;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.azure.AzureBlobStorage;
import com.emc.ecs.sync.storage.s3.EcsS3Storage;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.util.LoggingUtil;
import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.bean.VersioningConfiguration;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class AzureBlobStorageTest {
    private static final Logger log = LoggerFactory.getLogger(AzureBlobStorageTest.class);

    private final String bucketName = "ecs-sync-azure-s3-test";
    private CloudBlobClient blobClient;
    private String containerName;
    private S3Client s3;
    private AzureBlobStorage sourceStorage;
    private EcsS3Storage targetStorage;
    private AzureBlobConfig sourceConfig;
    private EcsS3Config targetConfig;

    private Level logLevel;

    @BeforeEach
    public void setup() throws Exception {
        logLevel = LoggingUtil.getRootLogLevel();
        // TODO: handle log elevation in a different way
        LoggingUtil.setRootLogLevel(Level.INFO);

        Properties syncProperties = TestConfig.getProperties();
        //for source
        String azureBlobConnectString = syncProperties.getProperty(TestConfig.PROP_AZURE_BLOB_CONNECT_STRING);
        containerName = syncProperties.getProperty(TestConfig.PROP_AZURE_BLOB_CONTAINER_NAME);
        Assumptions.assumeTrue(azureBlobConnectString != null && containerName != null);

        sourceConfig = new AzureBlobConfig();
        sourceConfig.setConnectionString(azureBlobConnectString);
        sourceConfig.setContainerName(containerName);

        sourceStorage = new AzureBlobStorage();
        sourceStorage.setConfig(sourceConfig);

        //for target
        String endpoint = syncProperties.getProperty(TestConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(TestConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(TestConfig.PROP_S3_SECRET_KEY);
        final boolean useVHost = Boolean.parseBoolean(syncProperties.getProperty(TestConfig.PROP_S3_VHOST));
        Assumptions.assumeTrue(endpoint != null && accessKey != null && secretKey != null);
        final URI endpointUri = new URI(endpoint);
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

//        EcsS3Config targetConfig = new EcsS3Config();
        targetConfig = new EcsS3Config();
        targetConfig.setProtocol(com.emc.ecs.sync.config.Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
        targetConfig.setHost(endpointUri.getHost());
        targetConfig.setPort(endpointUri.getPort());
        targetConfig.setEnableVHosts(useVHost);
        targetConfig.setAccessKey(accessKey);
        targetConfig.setSecretKey(secretKey);
        targetConfig.setBucketName(bucketName);
//        targetConfig.setIncludeVersions(true);

        targetStorage = new EcsS3Storage();
        targetStorage.setConfig(targetConfig);

        sourceStorage.configure(sourceStorage, null, targetStorage);
        blobClient = sourceStorage.getBlobClient();
    }

    public void tearDown() {
        LoggingUtil.setRootLogLevel(logLevel);
    }

    private CloudBlobContainer createContainer(final String containerName) throws URISyntaxException, StorageException {
        CloudBlobContainer container = blobClient.getContainerReference(containerName);
        container.createIfNotExists();
        return container;
    }

    @Test
    public void testListAllObjectInAzureBlob() throws URISyntaxException, StorageException {
        log.info("starting run testListAllObjectInAzureBlob");
        List<ObjectSummary> objectSummaries = new ArrayList<>();
        for (ObjectSummary summary : sourceStorage.allObjects()) {
            objectSummaries.add(summary);
        }
        log.info("object summaries: {}", objectSummaries.size());

        CloudBlobContainer container = blobClient.getContainerReference(containerName);
        List<CloudBlob> cloudBlobList = new ArrayList<>();
        for (ListBlobItem blob : container.listBlobs("", true)) {
            cloudBlobList.add((CloudBlob) blob);
            log.info("blob: {}", blob.getStorageUri().toString());
        }
        Assertions.assertEquals(objectSummaries.size(), cloudBlobList.size(), "blob number of sync mismatch with Azure blob");
    }

    @Test
    public void testLoadObjectWithoutSnapshots() throws URISyntaxException, StorageException, IOException, NoSuchAlgorithmException {
        log.info("Starting run testLoadObjectWithoutSnapshots");
        Random random = new Random();
        int bytesToStream = (128 * 1024) + random.nextInt(128 * 1024);
        byte[] sourceBytes = createUploadStream(bytesToStream);
        String sourceMd5Hex = DatatypeConverter.printHexBinary(MessageDigest.getInstance("MD5").digest(sourceBytes)).toLowerCase();
        log.info("source md5 hex: {}", sourceMd5Hex);
        InputStream inputStream = new ByteArrayInputStream(sourceBytes);

        String testBlobName = "test1/test2/testLoadObject.tmp";
        CloudBlobContainer container = blobClient.getContainerReference(containerName);
        CloudBlockBlob cloudBlob = container.getBlockBlobReference(testBlobName);
        cloudBlob.upload(inputStream, bytesToStream, null, null, null);

        SyncObject syncObject = sourceStorage.loadObject(testBlobName);
        String syncMd5Bytes = syncObject.getMd5Hex(true).toLowerCase();
        log.info("sync md5 hex: {}", syncMd5Bytes);
        Assertions.assertEquals(sourceMd5Hex, syncMd5Bytes, "sync data mismatch with source");
    }

    private byte[] createUploadStream(int bytesToStream) {
        Random random = new Random();
        byte[] randomBytes = new byte[bytesToStream];
        random.nextBytes(randomBytes);
        return randomBytes;
    }

    @Test
    public void testRunSyncWithoutSnapshots() {
        SyncConfig config = new SyncConfig().withSource(sourceConfig);
        config.getOptions().withIgnoreInvalidAcls(true).withVerify(true).setRetryAttempts(0);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(config);
        sync.setTarget(targetStorage);
        sync.run();
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
    }

    @Test
    public void testRunSyncWithSnapshots() {
        sourceConfig.setIncludeSnapShots(true);
        targetConfig.setIncludeVersions(true);

        s3.setBucketVersioning(bucketName, new VersioningConfiguration().withStatus(VersioningConfiguration.Status.Enabled));

        SyncConfig config = new SyncConfig().withSource(sourceConfig);
        config.getOptions().withIgnoreInvalidAcls(true).withVerify(true).setRetryAttempts(0);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(config);
        sync.setTarget(targetStorage);
        sync.run();
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
    }
}

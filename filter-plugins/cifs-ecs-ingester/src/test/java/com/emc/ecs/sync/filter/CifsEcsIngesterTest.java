/*
 * Copyright (c) 2017-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.filter.CifsEcsConfig;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.test.TestUtil;
import com.emc.ecs.sync.util.EnhancedThreadPoolExecutor;
import com.emc.ecs.sync.util.SyncUtil;
import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.jersey.S3JerseyClient;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class CifsEcsIngesterTest {
    private static final Logger log = LoggerFactory.getLogger(CifsEcsIngesterTest.class);

    private EcsS3Config targetConfig;
    private S3Client s3;
    private String bucketName = "ecs-sync-cifs-ecs-test-bucket";

    @BeforeEach
    public void setup() throws Exception {
        Properties syncProperties = TestConfig.getProperties();
        String endpoint = syncProperties.getProperty(TestConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(TestConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(TestConfig.PROP_S3_SECRET_KEY);
        final boolean useVHost = Boolean.valueOf(syncProperties.getProperty(TestConfig.PROP_S3_VHOST));
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
        targetConfig = new EcsS3Config();
        targetConfig.setProtocol(com.emc.ecs.sync.config.Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
        targetConfig.setHost(endpointUri.getHost());
        targetConfig.setPort(endpointUri.getPort());
        targetConfig.setEnableVHosts(useVHost);
        targetConfig.setAccessKey(accessKey);
        targetConfig.setSecretKey(secretKey);
        targetConfig.setBucketName(bucketName);
    }

    @AfterEach
    public void teardown() {
        if (s3 != null) {
            deleteBucket(s3, bucketName);
            s3.destroy();
        }
    }

    @Test
    public void testCifsEcsIngest() throws Exception {
        String[] encodings = {
                null,
                "<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>0</size></encoding>",
                "<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>0</size></encoding>",
                "<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>26</size></encoding>"
        };
        String[] originalNames = {
                null,
                "foo",
                "long-name",
                null
        };
        String[] attributes = {
                null,
                "AQAEAABoAAAABAAAEAAAAAQAAAgAAACWGFvcxNLSAQUEAQAQAAAABAAACAAAABK2F4TF0tIBBQQCABAAAAAEAAAIAAAAErYXhMXS0gEFBAMAEAAAAAQAAAgAAAAStheExdLSAQUEBAAEAAAAEAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAAAAAAAAAAAAUEAQAQAAAABAAACAAAAAAAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAQUF",
                "AQAEAABoAAAABAAAEAAAAAQAAAgAAAAiO/mKxdLSAQUEAQAQAAAABAAACAAAACI7+YrF0tIBBQQCABAAAAAEAAAIAAAA4hkKicXS0gEFBAMAEAAAAAQAAAgAAACeQQCJxtLSAQUEBAAEAAAAIAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAYAAAAAAAAAAUEAQAQAAAABAAACAAAABcAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAAUF",
                "AQAEAABoAAAABAAAEAAAAAQAAAgAAACajhqLxdLSAQUEAQAQAAAABAAACAAAAJqOGovF0tIBBQQCABAAAAAEAAAIAAAA4hkKicXS0gEFBAMAEAAAAAQAAAgAAAAuxwmJxtLSAQUEBAAEAAAAIAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAYAAAAAAAAAAUEAQAQAAAABAAACAAAABcAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAAUF"
        };
        String[] secDescriptors = {
                null,
                "AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAAQBAAAAAQAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAtAAHAAAAAAMYAP8BHwABAgAAAAAABSAAAAAgAgAAAAMUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAACxQAAAAAEAEBAAAAAAADAAAAAAADGACpABIAAQIAAAAAAAUgAAAAIQIAAAACGAAEAAAAAQIAAAAAAAUgAAAAIQIAAAACGAACAAAAAQIAAAAAAAUgAAAAIQIAAAU=",
                "AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAMAAAAC8AAAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAcAAEAAAAAAAYAP8BHwABAgAAAAAABSAAAAAgAgAAAAAUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAAABgAqQASAAECAAAAAAAFIAAAACECAAAF",
                "AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAMAAAAC8AAAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAcAAEAAAAAAAYAP8BHwABAgAAAAAABSAAAAAgAgAAAAAUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAAABgAqQASAAECAAAAAAAFIAAAACECAAAF"
        };
        String[] options = {
                "<Options><AllLowerCase>0</AllLowerCase><PreserveFolderCase>0</PreserveFolderCase><AllFilesLowerCase>0</AllFilesLowerCase><ConfigAllLowerCase>0</ConfigAllLowerCase></Options>",
                null,
                null,
                null
        };

        String csv = String.format("\"/root\",\"\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n", nn(encodings[0]), nn(originalNames[0]), nn(attributes[0]), nn(secDescriptors[0]), nn(options[0])) +
                String.format("\"/root/cenlocal\",\"cenlocal\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n", nn(encodings[1]), nn(originalNames[1]), nn(attributes[1]), nn(secDescriptors[1]), nn(options[1])) +
                String.format("\"/root/cenlocal/test - Copy (2).txt\",\"cenlocal/test - Copy (2).txt\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n", nn(encodings[2]), nn(originalNames[2]), nn(attributes[2]), nn(secDescriptors[2]), nn(options[2])) +
                String.format("\"/root/cenlocal/test - Copy (3).txt\",\"cenlocal/test - Copy (3).txt\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"", nn(encodings[3]), nn(originalNames[3]), nn(attributes[3]), nn(secDescriptors[3]), nn(options[3]));

        SyncOptions syncOptions = new SyncOptions();

        com.emc.ecs.sync.config.storage.TestConfig testConfig = new com.emc.ecs.sync.config.storage.TestConfig();
        testConfig.setDiscardData(false);

        TestStorage testStorage = new TestStorage();
        testStorage.withConfig(testConfig).withOptions(syncOptions);

        // create 3 source objects
        byte[] data = "Hello CIFS-ECS Ingest!".getBytes(StandardCharsets.UTF_8);
        testStorage.createObject(new SyncObject(testStorage, "",
                new ObjectMetadata().withDirectory(true), new ByteArrayInputStream(new byte[0]), new ObjectAcl()));
        testStorage.createObject(new SyncObject(testStorage, "cenlocal",
                new ObjectMetadata().withDirectory(true), new ByteArrayInputStream(new byte[0]), new ObjectAcl()));
        testStorage.createObject(new SyncObject(testStorage, "cenlocal/test - Copy (2).txt",
                new ObjectMetadata().withContentLength(0), new ByteArrayInputStream(new byte[0]), new ObjectAcl()));
        testStorage.createObject(new SyncObject(testStorage, "cenlocal/test - Copy (3).txt",
                new ObjectMetadata().withContentLength(data.length), new ByteArrayInputStream(data), new ObjectAcl()));

        File listFile = File.createTempFile("cifs-ecs-test", "list");
        listFile.deleteOnExit();
        Files.write(listFile.toPath(), csv.getBytes(StandardCharsets.UTF_8));

        SyncConfig syncConfig = new SyncConfig().withOptions(syncOptions).withTarget(targetConfig)
                .withFilters(Collections.singletonList(new CifsEcsConfig()));
        syncConfig.getOptions().withRecursive(false).withVerify(true).withSourceListFile(listFile.getPath());

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        TestUtil.run(sync);

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(encodings.length, sync.getStats().getObjectsComplete());

        SyncStorage targetStorage = sync.getTarget();

        SyncObject object = targetStorage.loadObject(targetStorage.getIdentifier("_$folder$", false));
        Assertions.assertFalse(object.getMetadata().isDirectory());
        // check CIFS-ECS metadata
        Assertions.assertEquals(encodings[0], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_COMMON_ENCODING));
        Assertions.assertEquals(originalNames[0], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_LONGNAME));
        Assertions.assertEquals(attributes[0], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_ATTR));
        Assertions.assertEquals(secDescriptors[0], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_SECDESC));
        Assertions.assertEquals(options[0], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_COMMON_OPTIONS));

        object = targetStorage.loadObject(targetStorage.getIdentifier("cenlocal/_$folder$", false));
        Assertions.assertFalse(object.getMetadata().isDirectory());
        // check CIFS-ECS metadata
        Assertions.assertEquals(encodings[1], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_COMMON_ENCODING));
        Assertions.assertEquals(originalNames[1], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_LONGNAME));
        Assertions.assertEquals(attributes[1], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_ATTR));
        Assertions.assertEquals(secDescriptors[1], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_SECDESC));
        Assertions.assertEquals(options[1], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_COMMON_OPTIONS));

        object = targetStorage.loadObject(targetStorage.getIdentifier("cenlocal/test - Copy (2).txt", false));
        Assertions.assertFalse(object.getMetadata().isDirectory());
        // check CIFS-ECS metadata
        Assertions.assertEquals(encodings[2], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_COMMON_ENCODING));
        Assertions.assertEquals(originalNames[2], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_LONGNAME));
        Assertions.assertEquals(attributes[2], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_ATTR));
        Assertions.assertEquals(secDescriptors[2], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_SECDESC));
        Assertions.assertEquals(options[2], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_COMMON_OPTIONS));
        // check data
        Assertions.assertArrayEquals(new byte[0], SyncUtil.readAsBytes(object.getDataStream()));

        object = targetStorage.loadObject(targetStorage.getIdentifier("cenlocal/test - Copy (3).txt", false));
        Assertions.assertFalse(object.getMetadata().isDirectory());
        // check CIFS-ECS metadata
        Assertions.assertEquals(encodings[3], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_COMMON_ENCODING));
        Assertions.assertEquals(originalNames[3], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_LONGNAME));
        Assertions.assertEquals(attributes[3], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_ATTR));
        Assertions.assertEquals(secDescriptors[3], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_WINDOWS_SECDESC));
        Assertions.assertEquals(options[3], object.getMetadata().getUserMetadataValue(CifsEcsIngester.MD_KEY_COMMON_OPTIONS));
        // check data
        Assertions.assertArrayEquals(data, SyncUtil.readAsBytes(object.getDataStream()));
    }

    private static String nn(String value) {
        if (value == null) return "";
        return value;
    }

    void deleteBucket(final S3Client s3, final String bucket) {
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
}

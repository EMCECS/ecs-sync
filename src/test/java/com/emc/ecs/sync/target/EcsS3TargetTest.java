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
package com.emc.ecs.sync.target;

import com.emc.ecs.sync.model.object.FileSyncObject;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.FilesystemSource;
import com.emc.ecs.sync.test.RandomInputStream;
import com.emc.ecs.sync.test.StreamTestSyncObject;
import com.emc.ecs.sync.test.SyncConfig;
import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.util.ChecksumAlgorithm;
import com.emc.object.util.ChecksummedInputStream;
import com.emc.object.util.RunningChecksum;
import com.emc.rest.smart.ecs.Vdc;
import com.emc.rest.util.StreamUtil;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class EcsS3TargetTest {
    Logger log = LoggerFactory.getLogger(EcsS3TargetTest.class);

    String bucketName = "ecs-sync-ecs-s3-target-test-bucket";
    S3Client s3;
    EcsS3Target target;

    @Before
    public void setup() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
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

        s3 = new S3JerseyClient(s3Config);

        try {
            s3.createBucket(bucketName);
        } catch (S3Exception e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        target = new EcsS3Target();
        target.setEnableVHosts(useVHost);
        if (useVHost) {
            target.setEndpoint(endpointUri);
        } else {
            target.setProtocol(endpointUri.getScheme().toUpperCase());
            target.setPort(endpointUri.getPort());
            target.setVdcs(Collections.singletonList(new Vdc(endpointUri.getHost())));
        }
        target.setAccessKey(accessKey);
        target.setSecretKey(secretKey);
        target.setBucketName(bucketName);
        target.configure(null, null, target);
    }

    @After
    public void teardown() {
        deleteBucket(bucketName);
    }

    @Test
    public void testNormalUpload() throws Exception {
        String key = "normal-upload";
        long size = 512 * 1024; // 512KiB
        InputStream stream = new RandomInputStream(size);
        SyncObject object = new StreamTestSyncObject(key, key, stream, size);

        target.filter(object);

        // proper ETag means no MPU was performed
        Assert.assertEquals(object.getMd5Hex(true).toUpperCase(), s3.getObjectMetadata(bucketName, key).getETag().toUpperCase());
    }

    @Ignore // only perform this test on a co-located ECS!
    @Test
    public void testVeryLargeUploadStream() throws Exception {
        String key = "large-stream-upload";
        long size = 512L * 1024 * 1024 + 10; // 512MB + 10 bytes
        InputStream stream = new RandomInputStream(size);
        SyncObject object = new StreamTestSyncObject(key, key, stream, size);

        target.filter(object);

        // hyphen denotes an MPU
        Assert.assertTrue(s3.getObjectMetadata(bucketName, key).getETag().contains("-"));

        // should upload in series (single thread).. there's no way the stream can be split, so just verifying the MD5
        // should be sufficient. need to read the entire object since we can't use the ETag
        InputStream objectStream = s3.getObject(bucketName, key).getObject();
        ChecksummedInputStream md5Stream = new ChecksummedInputStream(objectStream, new RunningChecksum(ChecksumAlgorithm.MD5));
        byte[] buffer = new byte[128 * 1024];
        int c;
        do {
            c = md5Stream.read(buffer);
        } while (c >= 0);
        md5Stream.close();

        Assert.assertEquals(object.getMd5Hex(true).toUpperCase(), md5Stream.getChecksum().getValue().toUpperCase());
    }

    @Ignore // only perform this test on a co-located ECS!
    @Test
    public void testVeryLargeUploadFile() throws Exception {
        String key = "large-file-upload";
        long size = 512L * 1024 * 1024 + 10; // 512MB + 10 bytes
        InputStream stream = new RandomInputStream(size);

        // create temp file
        File tempFile = File.createTempFile(key, null);
        tempFile.deleteOnExit();
        StreamUtil.copy(stream, new FileOutputStream(tempFile), size);

        SyncObject object = new FileSyncObject(new FilesystemSource(), new MimetypesFileTypeMap(), tempFile, key, false);

        target.filter(object);

        // hyphen denotes an MPU
        Assert.assertTrue(s3.getObjectMetadata(bucketName, key).getETag().contains("-"));

        // should upload in series (single thread).. there's no way the stream can be split, so just verifying the MD5
        // should be sufficient. need to read the entire object since we can't use the ETag
        InputStream objectStream = s3.getObject(bucketName, key).getObject();
        ChecksummedInputStream md5Stream = new ChecksummedInputStream(objectStream, new RunningChecksum(ChecksumAlgorithm.MD5));
        byte[] buffer = new byte[128 * 1024];
        int c;
        do {
            c = md5Stream.read(buffer);
        } while (c >= 0);
        md5Stream.close();

        Assert.assertEquals(object.getMd5Hex(true).toUpperCase(), md5Stream.getChecksum().getValue().toUpperCase());
    }

    private void deleteBucket(String bucket) {
        try {
            ListObjectsResult listing = null;
            do {
                if (listing == null) listing = s3.listObjects(bucket);
                else listing = s3.listMoreObjects(listing);

                List<String> keys = new ArrayList<>();
                for (S3Object summary : listing.getObjects()) {
                    keys.add(summary.getKey());
                }
                if (!keys.isEmpty())
                    s3.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(keys.toArray(new String[keys.size()])));
            } while (listing.isTruncated());

            s3.deleteBucket(bucket);
        } catch (RuntimeException e) {
            log.warn("could not delete bucket " + bucket, e);
        }
    }
}

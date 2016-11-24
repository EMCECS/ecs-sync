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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.emc.ecs.sync.model.object.FileSyncObject;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.FilesystemSource;
import com.emc.ecs.sync.test.RandomInputStream;
import com.emc.ecs.sync.test.StreamTestSyncObject;
import com.emc.ecs.sync.test.SyncConfig;
import com.emc.object.util.ChecksumAlgorithm;
import com.emc.object.util.ChecksummedInputStream;
import com.emc.object.util.RunningChecksum;
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
import java.util.List;
import java.util.Properties;

public class S3TargetTest {
    Logger log = LoggerFactory.getLogger(S3TargetTest.class);

    String bucketName = "ecs-sync-s3-target-test-bucket";
    AmazonS3 s3;
    S3Target s3Target;

    @Before
    public void setup() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
        String endpoint = syncProperties.getProperty(SyncConfig.PROP_S3_ENDPOINT);
        String accessKey = syncProperties.getProperty(SyncConfig.PROP_S3_ACCESS_KEY_ID);
        String secretKey = syncProperties.getProperty(SyncConfig.PROP_S3_SECRET_KEY);
        String proxyUri = syncProperties.getProperty(SyncConfig.PROP_HTTP_PROXY_URI);
        Assume.assumeNotNull(endpoint, accessKey, secretKey);

        ClientConfiguration config = new ClientConfiguration().withSignerOverride("S3SignerType");
        if (proxyUri != null) {
            URI uri = new URI(proxyUri);
            config.setProxyHost(uri.getHost());
            config.setProxyPort(uri.getPort());
        }

        s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), config);
        s3.setEndpoint(endpoint);
        try {
            s3.createBucket(bucketName);
        } catch (AmazonServiceException e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        s3Target = new S3Target();
        s3Target.setEndpoint(endpoint);
        s3Target.setAccessKey(accessKey);
        s3Target.setSecretKey(secretKey);
        s3Target.setLegacySignatures(true);
        s3Target.setBucketName(bucketName);
        s3Target.configure(null, null, s3Target);
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

        s3Target.filter(object);

        // proper ETag means no MPU was performed
        Assert.assertEquals(object.getMd5Hex(true).toLowerCase(), s3.getObjectMetadata(bucketName, key).getETag().toLowerCase());
    }

    @Ignore // only perform this test on a co-located S3 store!
    @Test
    public void testVeryLargeUploadStream() throws Exception {
        String key = "large-stream-upload";
        long size = 512L * 1024 * 1024 + 10; // 512MB + 10 bytes
        InputStream stream = new RandomInputStream(size);
        SyncObject object = new StreamTestSyncObject(key, key, stream, size);

        s3Target.filter(object);

        // hyphen denotes an MPU
        Assert.assertTrue(s3.getObjectMetadata(bucketName, key).getETag().contains("-"));

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

        Assert.assertEquals(object.getMd5Hex(true).toLowerCase(), md5Stream.getChecksum().getValue().toLowerCase());
    }

    @Ignore // only perform this test on a co-located S3 store!
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

        s3Target.filter(object);

        // hyphen denotes an MPU
        Assert.assertTrue(s3.getObjectMetadata(bucketName, key).getETag().contains("-"));

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

        Assert.assertEquals(object.getMd5Hex(true).toLowerCase(), md5Stream.getChecksum().getValue().toLowerCase());
    }

    private void deleteBucket(String bucket) {
        try {
            ObjectListing listing = null;
            do {
                if (listing == null) listing = s3.listObjects(bucket);
                else listing = s3.listNextBatchOfObjects(listing);

                List<String> keys = new ArrayList<>();
                for (S3ObjectSummary summary : listing.getObjectSummaries()) {
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

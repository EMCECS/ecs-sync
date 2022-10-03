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
package com.emc.ecs.sync.storage;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.emc.ecs.sync.storage.s3.AwsS3LargeFileUploader;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.util.EnhancedThreadPoolExecutor;
import com.emc.ecs.sync.util.SharedThreadPoolBackedExecutor;
import com.emc.object.s3.LargeFileUploader;
import com.emc.object.s3.bean.MultipartPartETag;
import com.emc.object.s3.lfu.LargeFileMultipartSource;
import com.emc.object.s3.lfu.LargeFileUploaderResumeContext;
import com.emc.object.s3.lfu.PartMismatchException;
import com.emc.object.util.ProgressListener;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsS3LargeFileUploaderTest {
    private static final Logger log = LoggerFactory.getLogger(AwsS3LargeFileUploaderTest.class);

    private URI endpointUri;
    private String accessKey;
    private String secretKey;
    private String region;
    private AmazonS3 s3;
    private static String bucket = "aws-lfu-test";
    private static MockMultipartSource mockMultipartSource = new MockMultipartSource();

    @BeforeEach
    public void setup() throws Exception {
        Properties syncProperties = TestConfig.getProperties();
        String endpoint = syncProperties.getProperty(TestConfig.PROP_S3_ENDPOINT);
        accessKey = syncProperties.getProperty(TestConfig.PROP_S3_ACCESS_KEY_ID);
        secretKey = syncProperties.getProperty(TestConfig.PROP_S3_SECRET_KEY);
        region = syncProperties.getProperty(TestConfig.PROP_S3_REGION);
        Assumptions.assumeTrue(endpoint != null && accessKey != null && secretKey != null);
        endpointUri = new URI(endpoint);

        ClientConfiguration config = new ClientConfiguration().withSignerOverride("S3SignerType");

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withClientConfiguration(config)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));

        s3 = builder.build();

        if (!s3.doesBucketExistV2(bucket)) s3.createBucket(bucket);
    }

    @AfterAll
    public void teardown() {
        MultipartUploadListing multipartUploadListing = s3.listMultipartUploads(new ListMultipartUploadsRequest(bucket));
        for (MultipartUpload multipartUpload : multipartUploadListing.getMultipartUploads()) {
            s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, multipartUpload.getKey(), multipartUpload.getUploadId()));
        }

        for (S3ObjectSummary object : s3.listObjects(bucket).getObjectSummaries()) {
            s3.deleteObject(bucket, object.getKey());
        }

        if (s3.doesBucketExistV2(bucket)) s3.deleteBucket(bucket);
    }

    @Test
    public void testUploadLargeFile() throws Exception {
        String key = "large-file-uploader.bin";
        File file = File.createTempFile("large-file-uploader-test", null);
        file.deleteOnExit();
        OutputStream out = new FileOutputStream(file);
        out.write(mockMultipartSource.getTotalBytes());
        out.close();

        AwsS3LargeFileUploader uploader = new AwsS3LargeFileUploader(s3, bucket, key, file).withPartSize(mockMultipartSource.getPartSize())
                .withMpuThreshold(AwsS3LargeFileUploader.MIN_PART_SIZE).withPartSize(AwsS3LargeFileUploader.MIN_PART_SIZE);

        // multipart upload
        uploader.doMultipartUpload();

        Assertions.assertEquals(mockMultipartSource.getTotalSize(), uploader.getBytesTransferred());
        Assertions.assertEquals(mockMultipartSource.getMpuETag(), s3.getObject(bucket, key).getObjectMetadata().getETag());
    }

    @Test
    public void testLargeFileUploaderStream() {
        String key = "large-file-uploader-stream.bin";

        AwsS3LargeFileUploader uploader = new AwsS3LargeFileUploader(s3, bucket, key,
                new ByteArrayInputStream(mockMultipartSource.getTotalBytes()), mockMultipartSource.getTotalSize())
                .withMpuThreshold(AwsS3LargeFileUploader.MIN_PART_SIZE).withPartSize(AwsS3LargeFileUploader.MIN_PART_SIZE);
        uploader.setPartSize(AwsS3LargeFileUploader.MIN_PART_SIZE);

        uploader.doMultipartUpload();

        Assertions.assertEquals(mockMultipartSource.getTotalSize(), uploader.getBytesTransferred());
        Assertions.assertEquals(mockMultipartSource.getMpuETag(), s3.getObject(bucket, key).getObjectMetadata().getETag());
    }

    @Test
    public void testResumeMpuFromMultiPartSource() {
        String key = "myprefix/mpu-resume-test-mps";
        final long partSize = mockMultipartSource.getPartSize();

        // init MPU
        String uploadId = s3.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key)).getUploadId();

        // upload first 2 parts
        for (int partNum = 1; partNum <= 2; partNum++) {
            UploadPartRequest request = new UploadPartRequest().withBucketName(bucket).withKey(key)
                    .withUploadId(uploadId).withPartNumber(partNum).withPartSize(partSize)
                    .withInputStream(mockMultipartSource.getPartDataStream((partNum - 1) * partSize, partSize));
            s3.uploadPart(request);
        }

        try {
            s3.getObjectMetadata(bucket, key);
            Assertions.fail("Object should not exist because MPU upload is incomplete");
        } catch (AmazonS3Exception e) {
            Assertions.assertEquals(404, e.getStatusCode());
        }

        LargeFileUploaderResumeContext resumeContext = new LargeFileUploaderResumeContext().withUploadId(uploadId);
        AwsS3LargeFileUploader lfu = new AwsS3LargeFileUploader(s3, bucket, key, mockMultipartSource)
                .withPartSize(partSize).withMpuThreshold(mockMultipartSource.getTotalSize()).withResumeContext(resumeContext);
        lfu.doMultipartUpload();

        ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(bucket).withPrefix(key);
        // uploadId will not exist after CompleteMultipartUpload.
        Assertions.assertEquals(0, s3.listMultipartUploads(request).getMultipartUploads().size());
        // object is uploaded successfully
        ObjectMetadata objectMetadata = s3.getObjectMetadata(bucket, key);
        Assertions.assertEquals(mockMultipartSource.getTotalSize(), objectMetadata.getContentLength());
        Assertions.assertEquals(mockMultipartSource.getMpuETag(), objectMetadata.getETag());
    }

    @Test
    public void testResumeMpuFromMultiPartSourceWithSharedThreadPool() {
        String key = "myprefix/mpu-resume-test-mpu-shared-thread-pool";
        final long partSize = mockMultipartSource.getPartSize();

        // init MPU
        String uploadId = s3.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key)).getUploadId();

        // upload first 2 parts
        for (int partNum = 1; partNum <= 2; partNum++) {
            UploadPartRequest request = new UploadPartRequest().withBucketName(bucket).withKey(key)
                    .withUploadId(uploadId).withPartNumber(partNum).withPartSize(partSize)
                    .withInputStream(mockMultipartSource.getPartDataStream((partNum - 1) * partSize, partSize));
            s3.uploadPart(request);
        }

        try {
            s3.getObjectMetadata(bucket, key);
            Assertions.fail("Object should not exist because MPU upload is incomplete");
        } catch (AmazonS3Exception e) {
            Assertions.assertEquals(404, e.getStatusCode());
        }

        EnhancedThreadPoolExecutor sharedExecutor = new EnhancedThreadPoolExecutor(4, new LinkedBlockingDeque<>(), "shared-pool");
        try {
            ExecutorService executor = new SharedThreadPoolBackedExecutor(sharedExecutor);

            LargeFileUploaderResumeContext resumeContext = new LargeFileUploaderResumeContext().withUploadId(uploadId);
            AwsS3LargeFileUploader lfu = new AwsS3LargeFileUploader(s3, bucket, key, mockMultipartSource)
                    .withPartSize(partSize).withMpuThreshold(mockMultipartSource.getTotalSize()).withResumeContext(resumeContext)
                    .withExecutorService(executor);
            lfu.doMultipartUpload();

            ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(bucket).withPrefix(key);
            // uploadId will not exist after CompleteMultipartUpload.
            Assertions.assertEquals(0, s3.listMultipartUploads(request).getMultipartUploads().size());
            // object is uploaded successfully
            ObjectMetadata objectMetadata = s3.getObjectMetadata(bucket, key);
            Assertions.assertEquals(mockMultipartSource.getTotalSize(), objectMetadata.getContentLength());
            Assertions.assertEquals(mockMultipartSource.getMpuETag(), objectMetadata.getETag());
        } finally {
            sharedExecutor.shutdownNow();
        }
    }

    @Test
    public void testResumeWithBadPartOverwrite() {
        testResumeWithBadPart(true);
    }

    @Test
    public void testResumeWithBadPartNoOverwrite() {
        testResumeWithBadPart(false);
    }

    private void testResumeWithBadPart(boolean overwriteBadPart) {
        String key = "mpu-resume-with-bad-part-data";
        MockMultipartSource mockMultipartSource = new MockMultipartSource();
        final long partSize = mockMultipartSource.getPartSize();
        int totalPartsToResume = 2;

        ByteProgressListener pl = new ByteProgressListener();

        // init MPU
        String uploadId = s3.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key)).getUploadId();

        // upload first 2 parts
        for (int partNum = 1; partNum <= totalPartsToResume; partNum++) {
            UploadPartRequest request = new UploadPartRequest().withBucketName(bucket).withKey(key)
                    .withUploadId(uploadId).withPartNumber(partNum).withPartSize(partSize)
                    .withInputStream(mockMultipartSource.getPartDataStream((partNum - 1) * partSize, partSize));
            s3.uploadPart(request);
            if (partNum == totalPartsToResume) {
                // simulate an uploaded part got wrong data
                byte[] data = new byte[(int) partSize];
                new Random().nextBytes(data);
                request = new UploadPartRequest().withBucketName(bucket).withKey(key).withUploadId(uploadId).withPartSize(partSize)
                        .withPartNumber(partNum).withInputStream(new ByteArrayInputStream(data));
                s3.uploadPart(request);
            }
        }

        LargeFileUploaderResumeContext resumeContext = new LargeFileUploaderResumeContext().withUploadId(uploadId);
        resumeContext.setOverwriteMismatchedParts(overwriteBadPart);
        AwsS3LargeFileUploader lfu = new AwsS3LargeFileUploader(s3, bucket, key, mockMultipartSource).withPartSize(partSize)
                .withMpuThreshold(mockMultipartSource.getTotalSize()).withResumeContext(resumeContext)
                .withProgressListener(pl);
        try {
            lfu.doMultipartUpload();
            if (!overwriteBadPart)
                Assertions.fail("one of the data in uploadedParts is wrong - should abort the upload and throw an exception");
        } catch (RuntimeException e) {
            if (overwriteBadPart) throw e;
            // root exception will be wrapped in ExecutionException and then RuntimeException
            Assertions.assertNotNull(e.getCause());
            Assertions.assertNotNull(e.getCause().getCause());
            Assertions.assertTrue(e.getCause().getCause() instanceof PartMismatchException);
            // make sure the failed part was expected
            Assertions.assertEquals(totalPartsToResume, ((PartMismatchException) e.getCause().getCause()).getPartNumber());
        }
        if (overwriteBadPart) {
            // should have re-uploaded the bad part, so this should be reflected in the bytes transferred
            Assertions.assertEquals(mockMultipartSource.getTotalSize() - (partSize * (totalPartsToResume - 1)), lfu.getBytesTransferred());
            Assertions.assertEquals(mockMultipartSource.getTotalSize() - (partSize * (totalPartsToResume - 1)), pl.completed.get());
            Assertions.assertEquals(mockMultipartSource.getTotalSize(), pl.total.get());
            Assertions.assertEquals(mockMultipartSource.getMpuETag(), lfu.getETag());
        } else {
            // MPU should be aborted because of a bad part
            try {
                s3.listParts(new ListPartsRequest(bucket, key, uploadId));
                Assertions.fail("UploadId should not exist because MPU is aborted");
            } catch (AmazonS3Exception e) {
                Assertions.assertEquals(404, e.getStatusCode());
                Assertions.assertEquals("NoSuchUpload", e.getErrorCode());
            }
        }
    }

    @Test
    public void testLargeFileUploaderProgressListener() throws Exception {
        String key = "large-file-uploader-progress-listener.bin";
        File file = File.createTempFile("large-file-uploader-test-progress-listener", null);
        file.deleteOnExit();
        OutputStream out = new FileOutputStream(file);
        out.write(mockMultipartSource.getTotalBytes());
        out.close();
        final AtomicLong completed = new AtomicLong();
        final AtomicLong total = new AtomicLong();
        final AtomicLong transferred = new AtomicLong();
        ProgressListener pl = new ProgressListener() {
            public void progress(long c, long t) {
                completed.set(c);
                total.set(t);
            }

            public void transferred(long size) {
                transferred.addAndGet(size);
            }
        };

        AwsS3LargeFileUploader uploader = new AwsS3LargeFileUploader(s3, bucket, key, file).withProgressListener(pl);
        uploader.setPartSize(AwsS3LargeFileUploader.MIN_PART_SIZE);

        // multipart
        uploader.doMultipartUpload();

        Assertions.assertEquals(mockMultipartSource.getTotalSize(), uploader.getBytesTransferred());
        Assertions.assertEquals(mockMultipartSource.getMpuETag(), s3.getObjectMetadata(bucket, key).getETag());
        Assertions.assertEquals(mockMultipartSource.getTotalSize(), completed.get());
        Assertions.assertEquals(mockMultipartSource.getTotalSize(), total.get());
        Assertions.assertTrue(transferred.get() >= mockMultipartSource.getTotalSize(), String.format("Should transfer at least %d bytes but only got %d", mockMultipartSource.getTotalSize(), transferred.get()));

        s3.deleteObject(bucket, key);
    }

    static class MockMultipartSource implements LargeFileMultipartSource {
        private static byte[] MPS_PARTS;
        private static final long partSize = AwsS3LargeFileUploader.MIN_PART_SIZE;
        private static final long totalSize = partSize * 4 + 123;

        public MockMultipartSource() {
            synchronized (MockMultipartSource.class) {
                if (MPS_PARTS == null) {
                    MPS_PARTS = new byte[(int) totalSize];
                    new Random().nextBytes(MPS_PARTS);
                }
            }
        }

        @Override
        public long getTotalSize() { return totalSize; }

        @Override
        public InputStream getCompleteDataStream() {
            return new ByteArrayInputStream(getTotalBytes());
        }

        @Override
        public InputStream getPartDataStream(long offset, long length) {
            return new ByteArrayInputStream(Arrays.copyOfRange(getTotalBytes(), (int) offset, (int) (offset + length)));
        }

        byte[] getTotalBytes() { return MPS_PARTS; }

        public long getPartSize() { return partSize; }

        public String getMpuETag() {
            List<MultipartPartETag> partETags = new ArrayList<>();
            int totalParts =(int) ((totalSize - 1) / partSize + 1);
            for (int i = 0; i < totalParts; i++) {
                int from = (int) partSize * i;
                int to = (int) (from + partSize);
                partETags.add(new MultipartPartETag(i + 1, DigestUtils.md5Hex(Arrays.copyOfRange(MPS_PARTS, from, to <= getTotalSize() ? to : (int)getTotalSize()))));
            }
            return LargeFileUploader.getMpuETag(partETags);
        }
    }

    static class ByteProgressListener implements ProgressListener {
        final AtomicLong completed = new AtomicLong();
        final AtomicLong total = new AtomicLong();
        final AtomicLong transferred = new AtomicLong();

        @Override
        public void progress(long c, long t) {
            completed.set(c);
            total.set(t);
        }

        @Override
        public void transferred(long size) {
            transferred.addAndGet(size);
        }
    }
}

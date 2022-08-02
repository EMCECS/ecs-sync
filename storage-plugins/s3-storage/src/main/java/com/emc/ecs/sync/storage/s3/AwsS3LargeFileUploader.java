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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.emc.ecs.sync.util.MultiValueMap;
import com.emc.object.s3.LargeFileUploader;
import com.emc.object.s3.S3Constants;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.CannedAcl;
import com.emc.object.s3.bean.MultipartPart;
import com.emc.object.s3.bean.MultipartPartETag;
import com.emc.object.s3.lfu.LargeFileMultipartFileSource;
import com.emc.object.s3.lfu.LargeFileMultipartSource;
import com.emc.object.s3.lfu.LargeFileUploaderResumeContext;
import com.emc.object.util.ProgressListener;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;

public class AwsS3LargeFileUploader extends LargeFileUploader {
    public static final long MIN_PART_SIZE = 5242880; // AWS S3 requires minimum 5 MiB in part size, but no limit on the last part.

    private final AmazonS3 s3;

    private ObjectMetadata objectMetadata;

    /**
     * An optional pre-configured access control policy to use for the new
     * object.  Ignored in favor of accessControlList, if present.
     */
    private CannedAccessControlList cannedAcl;

    /**
     * An optional access control list to apply to the new object. If specified,
     * cannedAcl will be ignored.
     */
    private AccessControlList acl;

    /**
     * Creates a new AwsS3LargeFileUpload instance using the specified <code>s3Client</code> to upload
     * <code>file</code> to <code>bucket/key</code>.
     */
    public AwsS3LargeFileUploader(AmazonS3 s3Client, String bucket, String key, File file) {
        this(s3Client, bucket, key, new LargeFileMultipartFileSource(file));
    }

    /**
     * Creates a new AwsS3LargeFileUpload instance using the specified <code>s3Client</code> to upload
     * from a single <code>stream</code> to <code>bucket/key</code>. Note that this type of upload is
     * single-threaded and not very efficient.
     */
    public AwsS3LargeFileUploader(AmazonS3 s3Client, String bucket, String key, InputStream stream, long size) {
        super(null, bucket, key, stream, size);
        this.s3 =s3Client;
    }

    /**
     * Creates a new AwsS3LargeFileUpload instance using the specified <code>s3Client</code> to upload
     * from a <code>multipartSource</code> to <code>bucket/key</code>.
     *
     * @see LargeFileMultipartSource
     */
    public AwsS3LargeFileUploader(AmazonS3 s3Client, String bucket, String key, LargeFileMultipartSource multipartSource) {
        super(null, bucket, key, multipartSource);
        this.s3 = s3Client;
    }

    @Override
    protected long getMinPartSize() {
        return MIN_PART_SIZE;
    }

    public ObjectMetadata getAwsS3ObjectMetadata() {
        return objectMetadata;
    }

    public void setAwsS3ObjectMetadata(ObjectMetadata objectMetadata) {
        this.objectMetadata = objectMetadata;
    }

    public AwsS3LargeFileUploader withAwsS3ObjectMetadata(ObjectMetadata objectMetadata) {
        setAwsS3ObjectMetadata(objectMetadata);
        return this;
    }

    @Override
    public void setObjectMetadata(S3ObjectMetadata objectMetadata) {
        throw new UnsupportedOperationException("cannot set ECS object metadata on an AWS uploader");
    }

    private AccessControlList getAwsS3Acl() {
        return acl;
    }

    public void setAwsS3Acl(AccessControlList acl) {
        this.acl = acl;
    }

    public AwsS3LargeFileUploader withAwsS3Acl(AccessControlList acl) {
        setAwsS3Acl(acl);
        return this;
    }

    @Override
    public void setAcl(com.emc.object.s3.bean.AccessControlList acl) {
        throw new UnsupportedOperationException("cannot set ECS ACL on an AWS uploader");
    }

    private CannedAccessControlList getAwsS3CannedAcl() {
        return cannedAcl;
    }

    public void setAwsS3CannedAcl(CannedAccessControlList cannedAcl) {
        this.cannedAcl = cannedAcl;
    }

    public AwsS3LargeFileUploader withAwsS3CannedAcl(CannedAccessControlList cannedAcl) {
        setAwsS3CannedAcl(cannedAcl);
        return this;
    }

    @Override
    public void setCannedAcl(CannedAcl cannedAcl) {
        throw new UnsupportedOperationException("cannot set ECS canned ACL on an AWS uploader");
    }

    public AwsS3LargeFileUploader withProgressListener(ProgressListener progressListener) {
        setProgressListener(progressListener);
        return this;
    }

    @Override
    public AwsS3LargeFileUploader withMpuThreshold(long mpuThreshold) {
        setMpuThreshold(mpuThreshold);
        return this;
    }

    @Override
    public AwsS3LargeFileUploader withPartSize(Long partSize) {
        setPartSize(partSize);
        return this;
    }

    @Override
    public AwsS3LargeFileUploader withResumeContext(LargeFileUploaderResumeContext resumeContext) {
        setResumeContext(resumeContext);
        return this;
    }

    @Override
    public AwsS3LargeFileUploader withThreads(int threads) {
        setThreads(threads);
        return this;
    }

    @Override
    public AwsS3LargeFileUploader withExecutorService(ExecutorService executorService) {
        setExecutorService(executorService);
        return this;
    }

    @Override
    protected String putObject(InputStream is) {
        PutObjectRequest request = new PutObjectRequest(getBucket(), getKey(), is, getAwsS3ObjectMetadata());
        request.setAccessControlList(getAwsS3Acl());
        request.setCannedAcl(getAwsS3CannedAcl());
        PutObjectResult result = s3.putObject(request);
        return result.getETag();
    }

    @Override
    protected List<MultipartPart> listParts(String uploadId) {
        List<MultipartPart> partList = new ArrayList<>();
        ListPartsRequest request = new ListPartsRequest(getBucket(), getKey(), uploadId);
        PartListing partListing = null;
        do {
            if (partListing != null) request.withPartNumberMarker(partListing.getNextPartNumberMarker());
            partListing = s3.listParts(request);
            for (PartSummary part : partListing.getParts()) {
                MultipartPart mpp = new MultipartPart();
                mpp.setPartNumber(part.getPartNumber());
                mpp.setETag(part.getETag());
                mpp.setSize(part.getSize());
                mpp.setLastModified(part.getLastModified());
                partList.add(mpp);
            }
        } while (partListing.isTruncated());

        return partList;
    }

    @Override
    protected String initMpu() {
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(getBucket(), getKey());
        initRequest.setObjectMetadata(getAwsS3ObjectMetadata());
        initRequest.setAccessControlList(getAwsS3Acl());
        initRequest.setCannedACL(getAwsS3CannedAcl());
        return s3.initiateMultipartUpload(initRequest).getUploadId();
    }

    @Override
    protected MultipartPartETag uploadPart(String uploadId, int partNumber, InputStream is, long length) {
        UploadPartRequest request = new UploadPartRequest().withBucketName(getBucket()).withKey(getKey()).withUploadId(uploadId).withPartSize(length)
                        .withPartNumber(partNumber).withInputStream(is);
        UploadPartResult result = s3.uploadPart(request);
        return new MultipartPartETag(result.getPartNumber(), result.getETag());
    }

    @Override
    protected com.emc.object.s3.bean.CompleteMultipartUploadResult completeMpu(String uploadId, SortedSet<MultipartPartETag> parts) {
        List<PartETag> partETags = new ArrayList<>();
        for (MultipartPartETag part : parts) {
            partETags.add(new PartETag(part.getPartNumber(), part.getETag()));
        }
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(getBucket(), getKey(), uploadId, partETags);
        CompleteMultipartUploadResult result = s3.completeMultipartUpload(request);

        // translate to ECS result
        MultiValueMap<String, String> headers = new MultiValueMap<>();
        headers.putSingle(S3Constants.AMZ_VERSION_ID, result.getVersionId());
        com.emc.object.s3.bean.CompleteMultipartUploadResult ecsResult = new com.emc.object.s3.bean.CompleteMultipartUploadResult();
        ecsResult.setBucketName(result.getBucketName());
        ecsResult.setKey(result.getKey());
        ecsResult.setHeaders(headers);
        ecsResult.setLocation(result.getLocation());
        ecsResult.setETag(result.getETag());
        return ecsResult;
    }

    @Override
    protected void abortMpu(String uploadId) {
        s3.abortMultipartUpload(new AbortMultipartUploadRequest(getBucket(), getKey(), uploadId));
    }

    @Override
    public void doByteRangeUpload() {
        throw new UnsupportedOperationException("Byte Range Upload is not supported in AWS uploader");
    }
}

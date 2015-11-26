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
package com.emc.ecs.sync.model.object;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.model.Checksum;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.util.AwsS3Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class S3SyncObject extends AbstractSyncObject<String> {
    private static final Logger log = LoggerFactory.getLogger(S3SyncObject.class);

    protected SyncPlugin parentPlugin;
    protected AmazonS3 s3;
    protected String bucketName;
    protected String key;
    protected Long size;

    public S3SyncObject(SyncPlugin parentPlugin, AmazonS3 s3, String bucketName, String key, String relativePath) {
        this(parentPlugin, s3, bucketName, key, relativePath, null);
    }

    public S3SyncObject(SyncPlugin parentPlugin, AmazonS3 s3, String bucketName, String key, String relativePath, Long size) {
        super(AwsS3Util.fullPath(bucketName, key), AwsS3Util.fullPath(bucketName, key), relativePath, false);
        this.parentPlugin = parentPlugin;
        this.s3 = s3;
        this.bucketName = bucketName;
        this.key = key;
        this.size = size;
    }

    @Override
    public synchronized boolean isDirectory() {
        try {
            checkLoaded();
            return AwsS3Util.isDirectoryPlaceholder(this);
        } catch (RuntimeException e) {
            // method contract states we cannot throw an exception
            log.info("cannot pull metadata to determine directory status: {}", e.toString());
            return false;
        }
    }

    @Override
    public boolean isLargeObject(int threshold) {
        return (size != null && size > threshold);
    }

    @Override
    public String getRelativePath() {
        if (isDirectory() && relativePath.length() > 0 && relativePath.charAt(relativePath.length() - 1) == '/') {
            return relativePath.substring(0, relativePath.length() - 1);
        } else {
            return relativePath;
        }
    }

    @Override
    public InputStream createSourceInputStream() {
        if (isDirectory()) return null;
        return new BufferedInputStream(s3.getObject(bucketName, key).getObjectContent(), parentPlugin.getBufferSize());
    }

    @Override
    protected void loadObject() {

        // load metadata
        ObjectMetadata s3meta = s3.getObjectMetadata(bucketName, key);
        SyncMetadata meta = toSyncMeta(s3meta);

        if (parentPlugin.isIncludeAcl()) {
            meta.setAcl(AwsS3Util.syncAclFromS3Acl(s3.getObjectAcl(bucketName, key)));
        }

        metadata = meta;
    }

    protected SyncMetadata toSyncMeta(ObjectMetadata s3meta) {
        SyncMetadata meta = new SyncMetadata();

        meta.setCacheControl(s3meta.getCacheControl());
        meta.setContentDisposition(s3meta.getContentDisposition());
        meta.setContentEncoding(s3meta.getContentEncoding());
        if (s3meta.getContentMD5() != null) meta.setChecksum(new Checksum("MD5", s3meta.getContentMD5()));
        meta.setContentType(s3meta.getContentType());
        meta.setHttpExpires(s3meta.getHttpExpiresDate());
        meta.setExpirationDate(s3meta.getExpirationTime());
        meta.setModificationTime(s3meta.getLastModified());
        meta.setContentLength(s3meta.getContentLength());
        meta.setUserMetadata(toMetaMap(s3meta.getUserMetadata()));

        return meta;
    }

    protected Map<String, SyncMetadata.UserMetadata> toMetaMap(Map<String, String> sourceMap) {
        Map<String, SyncMetadata.UserMetadata> metaMap = new HashMap<>();
        for (String key : sourceMap.keySet()) {
            metaMap.put(key, new SyncMetadata.UserMetadata(key, sourceMap.get(key)));
        }
        return metaMap;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getKey() {
        return key;
    }
}

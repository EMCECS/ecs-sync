/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.model.object;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.emc.vipr.sync.SyncPlugin;
import com.emc.vipr.sync.model.Checksum;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.util.S3Util;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class S3SyncObject extends AbstractSyncObject<String> {
    private SyncPlugin parentPlugin;
    private AmazonS3 s3;
    private String bucketName;

    public S3SyncObject(SyncPlugin parentPlugin, AmazonS3 s3, String bucketName, String key, String relativePath, boolean isCommonPrefix) {
        super(key, key, relativePath, isCommonPrefix);
        this.parentPlugin = parentPlugin;
        this.s3 = s3;
        this.bucketName = bucketName;
    }

    @Override
    public InputStream createSourceInputStream() {
        if (isDirectory()) return null;
        return new BufferedInputStream(s3.getObject(bucketName, sourceIdentifier).getObjectContent(), parentPlugin.getBufferSize());
    }

    @Override
    protected void loadObject() {
        if (isDirectory()) return;

        // load metadata
        ObjectMetadata s3meta = s3.getObjectMetadata(bucketName, sourceIdentifier);
        SyncMetadata meta = new SyncMetadata();

        meta.setChecksum(new Checksum("MD5", s3meta.getContentMD5()));
        meta.setContentType(s3meta.getContentType());
        meta.setExpirationDate(s3meta.getExpirationTime());
        meta.setModificationTime(s3meta.getLastModified());
        meta.setSize(s3meta.getContentLength());
        meta.setUserMetadata(toMetaMap(s3meta.getUserMetadata()));

        if (parentPlugin.isIncludeAcl()) {
            meta.setAcl(S3Util.syncAclFromS3Acl(s3.getObjectAcl(bucketName, sourceIdentifier)));
        }

        metadata = meta;
    }

    private Map<String, SyncMetadata.UserMetadata> toMetaMap(Map<String, String> sourceMap) {
        Map<String, SyncMetadata.UserMetadata> metaMap = new HashMap<String, SyncMetadata.UserMetadata>();
        for (String key : sourceMap.keySet()) {
            metaMap.put(key, new SyncMetadata.UserMetadata(key, sourceMap.get(key)));
        }
        return metaMap;
    }
}

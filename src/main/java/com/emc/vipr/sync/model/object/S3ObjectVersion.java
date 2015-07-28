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
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.emc.vipr.sync.SyncPlugin;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.util.S3Util;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Date;

public class S3ObjectVersion extends S3SyncObject {
    private String versionId;
    private boolean latest;
    private boolean deleteMarker;
    private Date lastModified;
    private String eTag;

    public S3ObjectVersion(SyncPlugin parentPlugin, AmazonS3 s3, String bucketName, String key, String versionId,
                           boolean latest, boolean deleteMarker, Date lastModified, String eTag, String relativePath) {
        super(parentPlugin, s3, bucketName, key, relativePath, false);
        // TODO: remove for ECS 2.0.1
        if (versionId == null || versionId.equals("null")) versionId = "0";
        this.versionId = versionId;
        this.latest = latest;
        this.deleteMarker = deleteMarker;
        this.lastModified = lastModified;
        this.eTag = eTag;
    }

    @Override
    public InputStream createSourceInputStream() {
        if (isDirectory()) return null;
        return new BufferedInputStream(s3.getObject(new GetObjectRequest(bucketName, key, versionId)).getObjectContent(),
                parentPlugin.getBufferSize());
    }

    @Override
    protected void loadObject() {
        if (isDirectory()) return;

        // load metadata
        ObjectMetadata s3meta = s3.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key, versionId));
        SyncMetadata meta = toSyncMeta(s3meta);

        if (parentPlugin.isIncludeAcl()) {
            meta.setAcl(S3Util.syncAclFromS3Acl(s3.getObjectAcl(bucketName, key, versionId)));
        }

        metadata = meta;
    }

    public String getVersionId() {
        return versionId;
    }

    public boolean isLatest() {
        return latest;
    }

    public boolean isDeleteMarker() {
        return deleteMarker;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public String getETag() {
        return eTag;
    }
}

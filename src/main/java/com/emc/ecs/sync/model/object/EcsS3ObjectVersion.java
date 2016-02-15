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

import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.util.EcsS3Util;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.request.GetObjectAclRequest;
import com.emc.object.s3.request.GetObjectMetadataRequest;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Date;

public class EcsS3ObjectVersion extends EcsS3SyncObject {
    private String versionId;
    private boolean latest;
    private boolean deleteMarker;
    private Date lastModified;
    private String eTag;

    public EcsS3ObjectVersion(SyncPlugin parentPlugin, S3Client s3, String bucketName, String key, String versionId,
                              boolean latest, boolean deleteMarker, Date lastModified, String eTag, String relativePath) {
        this(parentPlugin, s3, bucketName, key, versionId, latest, deleteMarker, lastModified, eTag, relativePath, null);
    }

    public EcsS3ObjectVersion(SyncPlugin parentPlugin, S3Client s3, String bucketName, String key, String versionId,
                              boolean latest, boolean deleteMarker, Date lastModified, String eTag, String relativePath,
                              Long size) {
        super(parentPlugin, s3, bucketName, key, relativePath, size);
        // TODO: remove for ECS 2.0.1
        if (versionId == null || versionId.equals("null")) versionId = "0";
        this.versionId = versionId;
        this.latest = latest;
        this.deleteMarker = deleteMarker;
        this.lastModified = lastModified;
        this.eTag = eTag;
    }

    @Override
    public synchronized boolean isDirectory() {
        return false;
    }

    @Override
    public InputStream createSourceInputStream() {
        if (isDirectory()) return null;

        InputStream inputStream = s3.readObject(bucketName, key, versionId, InputStream.class);
        return new BufferedInputStream(inputStream, parentPlugin.getBufferSize());
    }

    @Override
    protected void loadObject() {
        if (deleteMarker) return; // can't HEAD a delete marker

        // load metadata
        S3ObjectMetadata s3meta = s3.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key).withVersionId(versionId));
        metadata = toSyncMeta(s3meta);

        if (parentPlugin.isIncludeAcl()) {
            metadata.setAcl(EcsS3Util.syncAclFromS3Acl(s3.getObjectAcl(new GetObjectAclRequest(bucketName, key).withVersionId(versionId))));
        }
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

/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.storage.s3;

import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.azure.AzureBlobStorage;
import com.emc.ecs.sync.storage.azure.BlobSyncObject;
import org.apache.commons.compress.utils.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class S3ObjectVersion extends SyncObject {
    private static final Logger log = LoggerFactory.getLogger(S3ObjectVersion.class);

    private String versionId;
    private String eTag;
    private boolean latest;
    private boolean deleteMarker;

    public S3ObjectVersion(SyncStorage source, String relativePath, ObjectMetadata metadata) {
        super(source, relativePath, metadata);
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public S3ObjectVersion withVersionId(String versionId) {
        setVersionId(versionId);
        return this;
    }

    public String getETag() {
        return eTag;
    }

    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    public S3ObjectVersion withETag(String eTag) {
        setETag(eTag);
        return this;
    }

    public boolean isLatest() {
        return latest;
    }

    public void setLatest(boolean latest) {
        this.latest = latest;
    }

    public S3ObjectVersion withLatest(boolean latest) {
        setLatest(latest);
        return this;
    }

    public boolean isDeleteMarker() {
        return deleteMarker;
    }

    public void setDeleteMarker(boolean deleteMarker) {
        this.deleteMarker = deleteMarker;
    }

    public S3ObjectVersion withDeleteMarker(boolean deleteMarker) {
        setDeleteMarker(deleteMarker);
        return this;
    }

    @Override
    public void compareSyncObject(SyncObject syncObject) {
        if (syncObject instanceof BlobSyncObject
                && syncObject.getProperties().containsKey(AzureBlobStorage.PROP_BLOB_SNAPSHOTS)) {
            setProperty(AbstractS3Storage.PROR_OBJECT_SNAPSHOTS, true);
        }
    }

    /**
     * Generates a standard MD5 (from the object data) for individual versions, but for an instance that holds the entire
     * version list, generates an aggregate MD5 (of the individual MD5s) of all versions
     */
    @Override
    public String getMd5Hex(boolean forceRead) {
        // only the latest version (the one that is referenced by the ObjectContext) will have this property
        List versions = (List) getProperty(AbstractS3Storage.PROP_OBJECT_VERSIONS);
        if (versions == null) return super.getMd5Hex(forceRead);
        boolean isIncludedSnapshots = getProperty(AbstractS3Storage.PROR_OBJECT_SNAPSHOTS) != null;
        // build canonical string of all versions (deleteMarker, eTag) and hash it
        StringBuilder canonicalString = new StringBuilder("[");
        for (Object versionO : versions) {
            S3ObjectVersion version = (S3ObjectVersion) versionO;
            if (isIncludedSnapshots && version.isDeleteMarker()) {
                log.debug("version: {} with delete marker, need to skip when calculate md5", version.getVersionId());
                continue;
            }
            String md5 = (version == this) ? super.getMd5Hex(forceRead) : version.getMd5Hex(forceRead);
            canonicalString.append("{")
                    .append("\"deleteMarker\":").append(version.isDeleteMarker())
                    .append("\"md5\":\"").append(md5).append("\"")
                    .append("}");
        }
        canonicalString.append("]");
        try {
            MessageDigest digest = MessageDigest.getInstance("md5");
            return DatatypeConverter.printHexBinary(digest.digest(canonicalString.toString().getBytes(Charsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not initialize MD5", e);
        }
    }
}

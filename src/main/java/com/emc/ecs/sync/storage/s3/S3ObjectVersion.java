package com.emc.ecs.sync.storage.s3;

import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import org.apache.commons.compress.utils.Charsets;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class S3ObjectVersion extends SyncObject {
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

    /**
     * Generates a standard MD5 (from the object data) for individual versions, but for an instance that holds the entire
     * version list, generates an aggregate MD5 (of the individual MD5s) of all versions
     */
    @Override
    @SuppressWarnings("unchecked")
    public String getMd5Hex(boolean forceRead) {
        // only the latest version (the one that is referenced by the ObjectContext) will have this property
        List<S3ObjectVersion> versions = (List<S3ObjectVersion>) getProperty(AbstractS3Storage.PROP_OBJECT_VERSIONS);
        if (versions == null) return super.getMd5Hex(forceRead);

        // build canonical string of all versions (deleteMarker, eTag) and hash it
        StringBuilder canonicalString = new StringBuilder("[");
        for (S3ObjectVersion version : versions) {
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

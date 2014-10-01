package com.emc.vipr.sync.model;

import com.emc.atmos.api.bean.Metadata;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SyncMetadata {
    public static final String METADATA_DIR = ".viprmeta";
    public static final String DIR_META_FILE = ".dirmeta";

    protected String contentType;
    protected long size;
    protected Date modificationTime;
    protected Map<String, Object> userMetadata = new HashMap<>();
    protected SyncAcl acl;
    protected Checksum checksum;
    protected Date expirationDate;

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public Date getModificationTime() {
        return modificationTime;
    }

    public void setModificationTime(Date modificationTime) {
        this.modificationTime = modificationTime;
    }

    /**
     * WARNING: do not assume the values of this map will translate directly to a string (i.e. the
     * Atmos Metadata.toString() will print both the key and the value). if you want to get the string value of a key,
     * use {@link #getUserMetadataAsString(String)}.
     */
    public Map<String, Object> getUserMetadata() {
        return userMetadata;
    }

    /**
     * call this to conveniently get the string value of a key (the metadata map values may not translate directly).
     */
    public String getUserMetadataAsString(String key) {
        Object value = getUserMetadata().get(key);
        if (value == null) return null;
        if (value instanceof Metadata) return ((Metadata) value).getValue();
        return value.toString();
    }

    public void setUserMetadata(Map<String, Object> userMetadata) {
        this.userMetadata = userMetadata;
    }

    public SyncAcl getAcl() {
        return acl;
    }

    public void setAcl(SyncAcl acl) {
        this.acl = acl;
    }

    public Checksum getChecksum() {
        return checksum;
    }

    public void setChecksum(Checksum checksum) {
        this.checksum = checksum;
    }

    public Date getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(Date expirationDate) {
        this.expirationDate = expirationDate;
    }

    /**
     * For a given object path, returns the appropriate path that should contain that
     * objects's Metadata container.  This is a path/file with the same name inside the
     * .viprmeta subdirectory.  If the object itself is a directory, it's
     * ./.viprmeta/.dirmeta.
     *
     * @param relativePath the relative path of the object to compute the metadata path name from
     * @return the path that should contain this object's metadata.  This path may not exist.
     */
    public static String getMetaPath(String relativePath, boolean directory) {
        String name = new File(relativePath).getName();
        String base = directory ? relativePath : new File(relativePath).getParent();
        return new File(new File(base, METADATA_DIR), directory ? DIR_META_FILE : name).getPath();
    }

    public static SyncMetadata fromJson(String json) {
        return new GsonBuilder().serializeNulls().create().fromJson(json, SyncMetadata.class);
    }

    public final String toJson() {
        return new GsonBuilder().serializeNulls().create().toJson(this);
    }
}

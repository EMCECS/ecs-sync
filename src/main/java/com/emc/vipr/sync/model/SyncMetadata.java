package com.emc.vipr.sync.model;

import com.emc.vipr.sync.CommonOptions;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import javax.xml.bind.annotation.XmlTransient;
import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

public class SyncMetadata {
    public static final String METADATA_DIR = ".viprmeta";
    public static final String DIR_META_FILE = ".dirmeta";

    protected String instanceClass = SyncMetadata.class.getName();
    protected String contentType;
    protected long size;
    protected Date modificationTime;
    protected Map<String, UserMetadata> userMetadata = new TreeMap<String, UserMetadata>();
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

    public Map<String, UserMetadata> getUserMetadata() {
        return userMetadata;
    }

    @XmlTransient
    public Map<String, String> getUserMetadataValueMap() {
        Map<String, String> valueMap = new TreeMap<String, String>();
        for (String key : userMetadata.keySet()) {
            valueMap.put(key, userMetadata.get(key).getValue());
        }
        return valueMap;
    }

    public String getUserMetadataValue(String key) {
        UserMetadata meta = getUserMetadata().get(key);
        if (meta == null) return null;
        return meta.getValue();
    }

    public void setUserMetadata(Map<String, UserMetadata> userMetadata) {
        this.userMetadata = userMetadata;
    }

    public void setUserMetadataValue(String key, String value) {
        UserMetadata meta = getUserMetadata().get(key);
        if (meta == null) {
            setUserMetadataValue(key, value, false);
        } else {
            meta.setValue(value);
        }

    }

    public void setUserMetadataValue(String key, String value, boolean indexed) {
        getUserMetadata().put(key, new UserMetadata(key, value, indexed));
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
        JsonObject root = new JsonParser().parse(json).getAsJsonObject();
        JsonElement instanceClass = root.get("instanceClass");
        if (instanceClass == null) { // might be data from an older version; make sure user is aware
            throw new RuntimeException("could not parse meta-file. this could mean you used an older version of ViPRSync to move data to the source location. either use that same version again or use --" + CommonOptions.IGNORE_METADATA_OPTION + " to continue using this version");
        } else {
            try {
                return ((SyncMetadata) Class.forName(instanceClass.getAsString()).newInstance()).createFromJson(json);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    /**
     * Override in subclasses to add fidelity
     */
    protected SyncMetadata createFromJson(String json) {
        return new GsonBuilder().serializeNulls().create().fromJson(json, SyncMetadata.class);
    }

    public final String toJson() {
        return new GsonBuilder().serializeNulls().create().toJson(this);
    }

    public static class UserMetadata {
        private String key;
        private String value;
        private boolean indexed;

        public UserMetadata(String key, String value) {
            this(key, value, false);
        }

        public UserMetadata(String key, String value, boolean indexed) {
            this.key = key;
            this.value = value;
            this.indexed = indexed;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public boolean isIndexed() {
            return indexed;
        }

        public void setIndexed(boolean indexed) {
            this.indexed = indexed;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof UserMetadata)) return false;

            UserMetadata that = (UserMetadata) o;

            if (indexed != that.indexed) return false;
            if (!key.equals(that.key)) return false;
            if (value != null ? !value.equals(that.value) : that.value != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = key.hashCode();
            result = 31 * result + (value != null ? value.hashCode() : 0);
            result = 31 * result + (indexed ? 1 : 0);
            return result;
        }
    }
}

package com.emc.vipr.sync.model;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.File;
import java.util.Date;
import java.util.Set;

public abstract class SyncMetadata {
    public static final String METADATA_DIR = ".viprmeta";
    public static final String DIR_META_FILE = ".dirmeta";

    public static final String JSON_CLASS_PROPERTY = "metadataType";

    protected abstract void loadFromJson(JsonObject metaObject);

    protected abstract JsonObject toJsonObject();

    public abstract Set<String> getUserMetadataKeys();

    public abstract Set<String> getSystemMetadataKeys();

    public abstract String getUserMetadataProp(String key);

    public abstract String getSystemMetadataProp(String key);

    public abstract void setUserMetadataProp(String key, String value);

    public abstract Set<String> getUserAclKeys();

    public abstract Set<String> getGroupAclKeys();

    public abstract String getUserAclProp(String user);

    public abstract String getGroupAclProp(String group);

    public abstract void setUserAclProp(String user, String permission);

    public abstract void setGroupAclProp(String group, String permission);

    public abstract void removeUserAclProp(String user);

    public abstract void removeGroupAclProp(String group);

    public abstract String getContentType();

    public abstract void setContentType(String contentType);

    public abstract Date getModifiedTime();

    public abstract void setModifiedTime(Date modifiedTime);

    public abstract boolean isRetentionEnabled();

    public abstract void setRetentionEnabled(boolean retentionEnabled);

    public abstract Date getRetentionEndDate();

    public abstract void setRetentionEndDate(Date retentionEndDate);

    public abstract boolean isExpirationEnabled();

    public abstract void setExpirationEnabled(boolean expirationEnabled);

    public abstract Date getExpirationDate();

    public abstract void setExpirationDate(Date expirationDate);

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
        SyncMetadata metadata;
        JsonObject metaObject = (JsonObject) new JsonParser().parse(json);
        String className = metaObject.get(JSON_CLASS_PROPERTY).getAsString();
        try {
            metadata = (SyncMetadata) Class.forName(className).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Could not load metadata class", e);
        }
        metadata.loadFromJson(metaObject);
        return metadata;
    }

    public final String toJson() {
        JsonObject metaObject = toJsonObject();
        metaObject.addProperty(JSON_CLASS_PROPERTY, getClass().getName());
        return new GsonBuilder().serializeNulls().create().toJson(metaObject);
    }
}

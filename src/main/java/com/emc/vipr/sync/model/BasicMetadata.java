package com.emc.vipr.sync.model;

import com.emc.vipr.sync.util.Iso8601Util;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.*;

public class BasicMetadata extends SyncMetadata {
    private static final String USER_METADATA_PROP = "userMetadata";
    private static final String SYSTEM_METADATA_PROP = "systemMetadata";
    private static final String USER_ACL_PROP = "userAcl";
    private static final String GROUP_ACL_PROP = "groupAcl";
    private static final String CONTENT_TYPE_PROP = "contentType";
    private static final String MODIFIED_TIME_PROP = "modificationTime";
    private static final String RETENTION_ENABLED_PROP = "retentionEnabled";
    private static final String RETENTION_END_PROP = "retentionEndDate";
    private static final String EXPIRATION_ENABLED_PROP = "expirationEnabled";
    private static final String EXPIRATION_PROP = "expirationDate";

    private Map<String, String> systemMetadata = new TreeMap<String, String>();
    private Map<String, String> userMetadata = new TreeMap<String, String>();
    private Map<String, String> userAcl = new TreeMap<String, String>();
    private Map<String, String> groupAcl = new TreeMap<String, String>();
    private String contentType;
    private Date modifiedTime;
    private boolean retentionEnabled;
    private Date retentionEndDate;
    private boolean expirationEnabled;
    private Date expirationDate;

    public Map<String, String> getSystemMetadata() {
        return systemMetadata;
    }

    public void setSystemMetadata(Map<String, String> systemMetadata) {
        this.systemMetadata = systemMetadata;
    }

    public Map<String, String> getUserMetadata() {
        return userMetadata;
    }

    public void setUserMetadata(Map<String, String> userMetadata) {
        this.userMetadata = userMetadata;
    }

    public Map<String, String> getUserAcl() {
        return userAcl;
    }

    public void setUserAcl(Map<String, String> userAcl) {
        this.userAcl = userAcl;
    }

    public Map<String, String> getGroupAcl() {
        return groupAcl;
    }

    public void setGroupAcl(Map<String, String> groupAcl) {
        this.groupAcl = groupAcl;
    }

    @Override
    public Set<String> getUserMetadataKeys() {
        return userMetadata.keySet();
    }

    @Override
    public Set<String> getSystemMetadataKeys() {
        return systemMetadata.keySet();
    }

    @Override
    public String getUserMetadataProp(String key) {
        return userMetadata.get(key);
    }

    @Override
    public String getSystemMetadataProp(String key) {
        return systemMetadata.get(key);
    }

    @Override
    public void setUserMetadataProp(String key, String value) {
        userMetadata.put(key, value);
    }

    @Override
    public Set<String> getUserAclKeys() {
        return userAcl.keySet();
    }

    @Override
    public Set<String> getGroupAclKeys() {
        return groupAcl.keySet();
    }

    @Override
    public String getUserAclProp(String user) {
        return userAcl.get(user);
    }

    @Override
    public String getGroupAclProp(String group) {
        return groupAcl.get(group);
    }

    @Override
    public void setUserAclProp(String user, String permission) {
        userAcl.put(user, permission);
    }

    @Override
    public void setGroupAclProp(String group, String permission) {
        groupAcl.put(group, permission);
    }

    @Override
    public void removeUserAclProp(String user) {
        userAcl.remove(user);
    }

    @Override
    public void removeGroupAclProp(String group) {
        groupAcl.remove(group);
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public Date getModifiedTime() {
        return modifiedTime;
    }

    @Override
    public void setModifiedTime(Date modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    @Override
    public boolean isRetentionEnabled() {
        return retentionEnabled;
    }

    @Override
    public void setRetentionEnabled(boolean retentionEnabled) {
        this.retentionEnabled = retentionEnabled;
    }

    @Override
    public Date getRetentionEndDate() {
        return retentionEndDate;
    }

    @Override
    public void setRetentionEndDate(Date retentionEndDate) {
        this.retentionEndDate = retentionEndDate;
    }

    @Override
    public boolean isExpirationEnabled() {
        return expirationEnabled;
    }

    @Override
    public void setExpirationEnabled(boolean expirationEnabled) {
        this.expirationEnabled = expirationEnabled;
    }

    @Override
    public Date getExpirationDate() {
        return expirationDate;
    }

    @Override
    public void setExpirationDate(Date expirationDate) {
        this.expirationDate = expirationDate;
    }

    @Override
    protected void loadFromJson(JsonObject root) {
        userMetadata = loadMapFromJson(root.getAsJsonObject(USER_METADATA_PROP));
        systemMetadata = loadMapFromJson(root.getAsJsonObject(SYSTEM_METADATA_PROP));
        userAcl = loadMapFromJson(root.getAsJsonObject(USER_ACL_PROP));
        groupAcl = loadMapFromJson(root.getAsJsonObject(GROUP_ACL_PROP));

        JsonElement jContentType = root.get(CONTENT_TYPE_PROP);
        JsonElement jModifiedTime = root.get(MODIFIED_TIME_PROP);
        JsonElement jRetentionEnabled = root.get(RETENTION_ENABLED_PROP);
        JsonElement jRetentionEnd = root.get(RETENTION_END_PROP);
        JsonElement jExpirationEnabled = root.get(EXPIRATION_ENABLED_PROP);
        JsonElement jExpiration = root.get(EXPIRATION_PROP);

        contentType = jContentType == null ? null : jContentType.getAsString();
        modifiedTime = jModifiedTime == null ? null : Iso8601Util.parse(jModifiedTime.getAsString());
        retentionEnabled = jRetentionEnabled != null && jRetentionEnabled.getAsBoolean();
        retentionEndDate = jRetentionEnd == null ? null : Iso8601Util.parse(jRetentionEnd.getAsString());
        expirationEnabled = jExpirationEnabled != null && jExpirationEnabled.getAsBoolean();
        expirationDate = jExpiration == null ? null : Iso8601Util.parse(jExpiration.getAsString());
    }

    @Override
    protected JsonObject toJsonObject() {
        JsonObject root = new JsonObject();

        root.add(USER_METADATA_PROP, toJsonObject(userMetadata));
        root.add(SYSTEM_METADATA_PROP, toJsonObject(systemMetadata));
        root.add(USER_ACL_PROP, toJsonObject(userAcl));
        root.add(GROUP_ACL_PROP, toJsonObject(groupAcl));

        root.addProperty(CONTENT_TYPE_PROP, contentType);

        if (modifiedTime != null)
            root.addProperty(MODIFIED_TIME_PROP, Iso8601Util.format(modifiedTime));

        if (retentionEndDate != null) {
            root.addProperty(RETENTION_ENABLED_PROP, isRetentionEnabled());
            root.addProperty(RETENTION_END_PROP, Iso8601Util.format(retentionEndDate));
        }
        if (expirationDate != null) {
            root.addProperty(EXPIRATION_ENABLED_PROP, isExpirationEnabled());
            root.addProperty(EXPIRATION_PROP, Iso8601Util.format(expirationDate));
        }

        return root;
    }

    private Map<String, String> loadMapFromJson(JsonObject jmap) {
        Map<String, String> map = new HashMap<>();
        if (jmap != null) {
            for (Map.Entry<String, JsonElement> entry : jmap.entrySet()) {
                map.put(entry.getKey(), entry.getValue().getAsString());
            }
        }
        return map;
    }

    private JsonObject toJsonObject(Map<String, String> map) {
        JsonObject jmap = new JsonObject();
        if (map != null) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                jmap.addProperty(entry.getKey(), entry.getValue());
            }
        }
        return jmap;
    }
}

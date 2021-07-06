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
package com.emc.ecs.sync.model;

import com.emc.ecs.sync.util.SyncUtil;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import javax.xml.bind.annotation.XmlTransient;
import java.io.File;
import java.util.*;

public class ObjectMetadata implements Cloneable {
    public static final String METADATA_DIR = ".ecsmeta";
    public static final String DIR_META_FILE = ".dirmeta";

    protected String instanceClass = ObjectMetadata.class.getName();
    protected boolean directory;
    private String cacheControl;
    private String contentDisposition;
    private String contentEncoding;
    private long contentLength;
    private String contentType;
    private Date httpExpires;
    private Date modificationTime;
    private Date metaChangeTime;
    private Date accessTime;
    private Map<String, UserMetadata> userMetadata = new TreeMap<>();
    protected Checksum checksum;
    private Date expirationDate;
    private String retentionPolicy;
    private Date retentionEndDate;

    /**
     * Returns whether this object represents a directory or prefix. If false, assume this is a data object (even if
     * size is zero). Note that if you override this method, it *CANNOT* throw an exception. This would silently break
     * several logic flows.
     */
    public boolean isDirectory() {
        return directory;
    }

    public void setDirectory(boolean directory) {
        this.directory = directory;
    }

    public ObjectMetadata withDirectory(boolean directory) {
        setDirectory(directory);
        return this;
    }

    public String getCacheControl() {
        return cacheControl;
    }

    public void setCacheControl(String cacheControl) {
        this.cacheControl = cacheControl;
    }

    public ObjectMetadata withCacheControl(String cacheControl) {
        setCacheControl(cacheControl);
        return this;
    }

    public String getContentDisposition() {
        return contentDisposition;
    }

    public void setContentDisposition(String contentDisposition) {
        this.contentDisposition = contentDisposition;
    }

    public ObjectMetadata withContentDisposition(String contentDisposition) {
        setContentDisposition(contentDisposition);
        return this;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public void setContentEncoding(String contentEncoding) {
        this.contentEncoding = contentEncoding;
    }

    public ObjectMetadata withContentEncoding(String contentEncoding) {
        setContentEncoding(contentEncoding);
        return this;
    }

    public long getContentLength() {
        return contentLength;
    }

    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }

    public ObjectMetadata withContentLength(long contentLength) {
        setContentLength(contentLength);
        return this;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public ObjectMetadata withContentType(String contentType) {
        setContentType(contentType);
        return this;
    }

    public Date getHttpExpires() {
        return httpExpires;
    }

    public void setHttpExpires(Date httpExpires) {
        this.httpExpires = httpExpires;
    }

    public ObjectMetadata withHttpExpires(Date httpExpires) {
        setHttpExpires(httpExpires);
        return this;
    }

    public Date getModificationTime() {
        return modificationTime;
    }

    public void setModificationTime(Date modificationTime) {
        this.modificationTime = modificationTime;
    }

    public ObjectMetadata withModificationTime(Date modificationTime) {
        setModificationTime(modificationTime);
        return this;
    }

    public Date getMetaChangeTime() {
        return metaChangeTime;
    }

    public void setMetaChangeTime(Date metaChangeTime) {
        this.metaChangeTime = metaChangeTime;
    }

    public ObjectMetadata withMetaChangeTime(Date metaChangeTime) {
        setMetaChangeTime(metaChangeTime);
        return this;
    }

    public Date getAccessTime() {
        return accessTime;
    }

    public void setAccessTime(Date accessTime) {
        this.accessTime = accessTime;
    }

    public ObjectMetadata withAccessTime(Date accessTime) {
        setAccessTime(accessTime);
        return this;
    }

    public Map<String, UserMetadata> getUserMetadata() {
        return userMetadata;
    }

    @XmlTransient
    public Map<String, String> getUserMetadataValueMap() {
        return new StringView();
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

    public Checksum getChecksum() {
        return checksum;
    }

    public void setChecksum(Checksum checksum) {
        this.checksum = checksum;
    }

    public ObjectMetadata withChecksum(Checksum checksum) {
        setChecksum(checksum);
        return this;
    }

    public Date getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(Date expirationDate) {
        this.expirationDate = expirationDate;
    }

    public ObjectMetadata withExpirationDate(Date expirationDate) {
        setExpirationDate(expirationDate);
        return this;
    }

    public String getRetentionPolicy() {
        return retentionPolicy;
    }

    public void setRetentionPolicy(String retentionPolicy) {
        this.retentionPolicy = retentionPolicy;
    }

    public ObjectMetadata withRetentionPolicy(String retentionPolicy) {
        setRetentionPolicy(retentionPolicy);
        return this;
    }

    public Date getRetentionEndDate() {
        return retentionEndDate;
    }

    public void setRetentionEndDate(Date retentionEndDate) {
        this.retentionEndDate = retentionEndDate;
    }

    public ObjectMetadata withRetentionEndDate(Date retentionEndDate) {
        setRetentionEndDate(retentionEndDate);
        return this;
    }
    /**
     * For a given object path, returns the appropriate path that should contain that
     * object's Metadata container.  This is a path/file with the same name inside the
     * .ecsmeta subdirectory.  If the object itself is a directory, it's
     * ./.ecsmeta/.dirmeta.
     *
     * @param relativePath the relative path of the object to compute the metadata path name from
     * @return the path that should contain this object's metadata.  This path may not exist.
     */
    public static String getMetaPath(String relativePath, boolean directory) {
        String name = new File(relativePath).getName();
        String base = directory ? relativePath : new File(relativePath).getParent();
        return SyncUtil.combinedPath(SyncUtil.combinedPath(base, METADATA_DIR), directory ? DIR_META_FILE : name);
    }

    public static ObjectMetadata fromJson(String json) {
        JsonObject root = new JsonParser().parse(json).getAsJsonObject();
        JsonElement instanceClass = root.get("instanceClass");
        if (instanceClass == null) { // might be data from an older version; make sure user is aware
            throw new RuntimeException("could not parse meta-file. this could mean you used an older version of EcsSync to move data to the source location. either use that same version again or use --no-sync-user-metadata to continue using this version");
        } else {
            try {
                return ((ObjectMetadata) Class.forName(instanceClass.getAsString()).newInstance()).createFromJson(json);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    /**
     * Override in subclasses to add fidelity
     */
    protected ObjectMetadata createFromJson(String json) {
        return new GsonBuilder().serializeNulls().create().fromJson(json, ObjectMetadata.class);
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

    private class StringView implements Map<String, String> {
        @Override
        public int size() {
            return userMetadata.size();
        }

        @Override
        public boolean isEmpty() {
            return userMetadata.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return userMetadata.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            return userMetadata.containsValue(value);
        }

        @Override
        public String get(Object key) {
            return getUserMetadataValue((String) key);
        }

        @Override
        public String put(String key, String value) {
            String oldValue = get(key);
            setUserMetadataValue(key, value);
            return oldValue;
        }

        @Override
        public String remove(Object key) {
            String oldValue = get(key);
            userMetadata.remove(key);
            return oldValue;
        }

        @Override
        public void putAll(Map<? extends String, ? extends String> m) {
            for (String key : m.keySet()) {
                put(key, m.get(key));
            }
        }

        @Override
        public void clear() {
            userMetadata.clear();
        }

        @Override
        public Set<String> keySet() {
            return userMetadata.keySet();
        }

        @Override
        public Collection<String> values() {
            throw new UnsupportedOperationException("cannot get values collection");
        }

        @Override
        public Set<Entry<String, String>> entrySet() {
            throw new UnsupportedOperationException("cannot get entry set");
        }
    }
}

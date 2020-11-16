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

import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.EnhancedInputStream;
import com.emc.ecs.sync.util.LazyValue;
import com.emc.ecs.sync.util.PerformanceListener;
import com.emc.ecs.sync.util.SyncUtil;
import com.emc.object.util.ProgressInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class SyncObject implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SyncObject.class);

    private SyncStorage source;
    private String relativePath;
    private ObjectMetadata metadata;
    private EnhancedInputStream enhancedStream;
    private ObjectAcl acl;
    private boolean postStreamUpdateRequired;
    private Map<String, Object> properties = new HashMap<>();
    private byte[] md5;
    private LazyValue<InputStream> lazyStream;
    private LazyValue<ObjectAcl> lazyAcl;
    private long bytesRead;

    public SyncObject(SyncStorage source, String relativePath, ObjectMetadata metadata) {
        this(source, relativePath, metadata, null, null);
    }

    public SyncObject(SyncStorage source, String relativePath, ObjectMetadata metadata, InputStream dataStream, ObjectAcl acl) {
        assert source != null : "source cannot be null";
        assert relativePath != null : "relativePath cannot be null";
        assert metadata != null : "metadata cannot be null";
        this.source = source;
        this.relativePath = relativePath;
        this.metadata = metadata;
        if (dataStream != null) setDataStream(dataStream);
        this.acl = acl;
    }

    public SyncStorage getSource() {
        return source;
    }

    /**
     * Gets the relative path for the object.  If the target is a
     * namespace target, this path will be used when computing the
     * absolute path in the target, relative to the target root.
     */
    public String getRelativePath() {
        return relativePath;
    }

    /**
     * Sets the relative path for the object.  If the target is a
     * namespace target, this path will be used when computing the
     * absolute path in the target, relative to the target root.
     */
    public void setRelativePath(String relativePath) {
        this.relativePath = relativePath;
    }

    public ObjectMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(ObjectMetadata metadata) {
        this.metadata = metadata;
    }

    public synchronized InputStream getDataStream() {
        if (enhancedStream == null && lazyStream != null) {
            setDataStream(lazyStream.get());
        }
        return enhancedStream;
    }

    public void setDataStream(InputStream dataStream) {
        if (dataStream == null) {
            enhancedStream = null;
        } else {
            wrap(dataStream);
        }
    }

    public void setLazyStream(LazyValue<InputStream> lazyStream) {
        this.lazyStream = lazyStream;
    }

    public synchronized ObjectAcl getAcl() {
        if (acl == null && lazyAcl != null) {
            setAcl(lazyAcl.get());
        }
        return acl;
    }

    public void setAcl(ObjectAcl acl) {
        this.acl = acl;
    }

    public void setLazyAcl(LazyValue<ObjectAcl> lazyAcl) {
        this.lazyAcl = lazyAcl;
    }

    public boolean isPostStreamUpdateRequired() {
        return postStreamUpdateRequired;
    }

    /**
     * Specifies whether this object has stream-dependent metadata (metadata that changes after the object data is
     * streamed).  I.e. the encryption filter will add a checksum, unencrypted size and signature.
     * <p>
     * If set, the object's metadata will be updated in the target after its data is transferred.
     * <p>
     * Default is false.  Set to true if your object has metadata that changes after its data is streamed.
     */
    public void setPostStreamUpdateRequired(boolean postStreamUpdateRequired) {
        this.postStreamUpdateRequired = postStreamUpdateRequired;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        assert properties != null : "properties must not be null";
        this.properties = properties;
    }

    public Object getProperty(String name) {
        return properties.get(name);
    }

    public void setProperty(String name, Object value) {
        properties.put(name, value);
    }

    public void removeProperty(String name) {
        properties.remove(name);
    }

    public long getBytesRead() {
        if (bytesRead > 0) {
            return bytesRead;
        } else if (enhancedStream != null) {
            return enhancedStream.getBytesRead();
        } else {
            return 0;
        }
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public String getMd5Hex(boolean forceRead) {
        byte[] md5 = getMd5(forceRead);
        if (md5 == null) return null;
        return DatatypeConverter.printHexBinary(md5);
    }

    @Override
    public void close() throws Exception {
        try {
            if (enhancedStream != null) enhancedStream.close();
        } catch (Throwable t) {
            log.warn("could not close data stream", t);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SyncObject object = (SyncObject) o;

        return relativePath.equals(object.relativePath);

    }

    @Override
    public int hashCode() {
        return relativePath.hashCode();
    }

    private synchronized byte[] getMd5(boolean forceRead) {
        if (md5 == null) {
            if (forceRead) getDataStream(); // make sure lazy streams are initialized
            if (enhancedStream == null) return null;
            if (!enhancedStream.isClosed()) {
                if (!forceRead || enhancedStream.getBytesRead() > 0)
                    throw new IllegalStateException("Cannot call getMd5 until stream is closed");
                SyncUtil.consumeAndCloseStream(enhancedStream);
            }
            md5 = enhancedStream.getMd5Digest();
        }
        return md5;
    }

    private void wrap(InputStream dataStream) {
        if (source != null && source.getOptions().isMonitorPerformance())
            dataStream = new ProgressInputStream(dataStream, new PerformanceListener(source.getReadWindow()));
        enhancedStream = new EnhancedInputStream(dataStream, true);
    }

    public SyncObject withAcl(ObjectAcl acl) {
        setAcl(acl);
        return this;
    }

    public SyncObject withLazyStream(LazyValue<InputStream> lazyStream) {
        setLazyStream(lazyStream);
        return this;
    }

    public SyncObject withLazyAcl(LazyValue<ObjectAcl> lazyAcl) {
        setLazyAcl(lazyAcl);
        return this;
    }

    public void compareSyncObject(SyncObject syncObject) {};
}

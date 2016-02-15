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
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.util.CountingInputStream;
import com.emc.ecs.sync.util.SyncUtil;
import com.emc.ecs.sync.util.TransferProgressListener;
import com.emc.object.util.ProgressInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public abstract class AbstractSyncObject<I> implements SyncObject<I> {
    private Logger log = LoggerFactory.getLogger(AbstractSyncObject.class);

    private SyncPlugin parentPlugin;
    private final I rawSourceIdentifier;
    private final String sourceIdentifier;
    private final String relativePath;
    private final boolean directory;
    private String targetIdentifier;
    protected SyncMetadata metadata = new SyncMetadata();
    private boolean objectLoaded = false;
    private CountingInputStream cin;
    private DigestInputStream din;
    private byte[] md5;
    private int failureCount = 0;

    public AbstractSyncObject(SyncPlugin parentPlugin, I rawSourceIdentifier, String sourceIdentifier,
                              String relativePath, boolean directory) {
        Assert.notNull(sourceIdentifier, "sourceIdentifier cannot be null");
        this.parentPlugin = parentPlugin;
        this.rawSourceIdentifier = rawSourceIdentifier;
        this.sourceIdentifier = sourceIdentifier;
        this.relativePath = relativePath;
        this.directory = directory;
    }

    /**
     * Loads all necessary metadata for the object
     */
    protected abstract void loadObject();

    /**
     * Must always create a new InputStream and must not return null.
     */
    protected abstract InputStream createSourceInputStream();

    /**
     * Returns the plugin that generated this sync object. The parent plugin is referenced for tracking purposes
     */
    public SyncPlugin getParentPlugin() {
        return parentPlugin;
    }

    /**
     * Returns the source identifier in its raw form as implemented for the source system
     * (i.e. the Atmos ObjectIdentifier)
     */
    @Override
    public I getRawSourceIdentifier() {
        return rawSourceIdentifier;
    }

    /**
     * Returns the string representation of the full source identifier. Used in logging and reference updates.
     */
    @Override
    public String getSourceIdentifier() {
        return sourceIdentifier;
    }

    /**
     * Gets the relative path for the object.  If the target is a
     * namespace target, this path will be used when computing the
     * absolute path in the target, relative to the target root.
     */
    @Override
    public String getRelativePath() {
        return relativePath;
    }

    /**
     * Returns whether this object represents a directory or prefix. If false, assume this is a data object (even if
     * size is zero).
     */
    @Override
    public boolean isDirectory() {
        return directory;
    }

    /**
     * Used for discriminating thread pools. Override only if the source/target system requires larger thread counts for
     * smaller files *and* the size of the object can be determined at initialization (don't load metadata here!)
     */
    @Override
    public boolean isLargeObject(int threshold) {
        return false;
    }

    /**
     * Returns the string representation of the full target identifier. Used in logging and reference updates.
     */
    @Override
    public String getTargetIdentifier() {
        return targetIdentifier;
    }

    @Override
    public SyncMetadata getMetadata() {
        checkLoaded();
        return metadata;
    }

    /**
     * Specifies whether this object has stream-dependent metadata (metadata that changes after the object data is
     * streamed).  I.e. the encryption filter will add a checksum and unencrypted size.
     * <p/>
     * Targets must check this flag and, if set, update the object's metadata in the target after its data is
     * transferred.
     * <p/>
     * Default setting is false.  Override this method and return true if your object has metadata that changes after
     * its data is streamed.
     */
    @Override
    public boolean requiresPostStreamMetadataUpdate() {
        return false;
    }

    @Override
    public void setTargetIdentifier(String targetIdentifier) {
        this.targetIdentifier = targetIdentifier;
    }

    @Override
    public void setMetadata(SyncMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public final synchronized InputStream getInputStream() {
        try {
            if (cin == null) {
                InputStream in = din = new DigestInputStream(createSourceInputStream(), MessageDigest.getInstance("MD5"));
                if (parentPlugin != null && parentPlugin.isMonitorPerformance())
                    in = new ProgressInputStream(din, new TransferProgressListener(parentPlugin.getReadPerformanceCounter()));
                cin = new CountingInputStream(in);
            }
            return cin;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("No MD5 digest?!");
        }
    }

    @Override
    public long getBytesRead() {
        if (cin != null) {
            return cin.getBytesRead();
        } else {
            return 0;
        }
    }

    /**
     * Default implementation does nothing. **Override this method to close your object's resources!
     */
    @Override
    public void close() throws IOException {
        // no-op
    }

    protected synchronized byte[] getMd5(boolean forceRead) {
        if (md5 == null) {
            getInputStream();
            if (!cin.isClosed()) {
                if (!forceRead || cin.getBytesRead() > 0)
                    throw new IllegalStateException("Cannot call getMd5 until stream is closed");
                SyncUtil.consumeAndCloseStream(cin);
            }
            md5 = din.getMessageDigest().digest();
        }
        return md5;
    }

    @Override
    public String getMd5Hex(boolean forceRead) {
        return DatatypeConverter.printHexBinary(getMd5(forceRead));
    }

    @Override
    public void incFailureCount() {
        failureCount++;
    }

    @Override
    public int getFailureCount() {
        return failureCount;
    }

    /**
     * Override and call super to release implementation-specific resources
     */
    @Override
    public void reset() {
        if (cin != null) {
            try {
                cin.close();
            } catch (IOException e) {
                log.warn("could not close stream during reset", e);
            }
        }
        cin = null;
        din = null;
        metadata = new SyncMetadata();
        objectLoaded = false;
        md5 = null;
    }

    protected synchronized void checkLoaded() {
        if (!objectLoaded) {
            loadObject();
            objectLoaded = true;
        }
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", getClass().getSimpleName(), getRelativePath());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SyncObject that = (SyncObject) o;

        if (!getRelativePath().equals(that.getRelativePath())) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return getRelativePath().hashCode();
    }
}

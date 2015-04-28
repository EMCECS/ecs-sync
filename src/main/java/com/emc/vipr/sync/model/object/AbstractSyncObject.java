/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.model.object;

import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.util.CountingInputStream;
import com.emc.vipr.sync.util.SyncUtil;
import org.springframework.util.Assert;

import javax.xml.bind.DatatypeConverter;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public abstract class AbstractSyncObject<I> implements SyncObject<I> {
    protected final I rawSourceIdentifier;
    protected final String sourceIdentifier;
    protected final String relativePath;
    protected final boolean directory;
    protected String targetIdentifier;
    protected SyncMetadata metadata = new SyncMetadata();
    private boolean objectLoaded = false;
    private CountingInputStream cin;
    private DigestInputStream din;
    private byte[] md5;

    public AbstractSyncObject(I rawSourceIdentifier, String sourceIdentifier, String relativePath, boolean directory) {
        Assert.notNull(sourceIdentifier, "sourceIdentifier cannot be null");
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
    public synchronized boolean isDirectory() {
        return directory;
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
                din = new DigestInputStream(createSourceInputStream(), MessageDigest.getInstance("MD5"));
                cin = new CountingInputStream(din);
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

    protected synchronized byte[] getMd5(boolean forceRead) {
        if (md5 == null) {
            getInputStream();
            if (!cin.isClosed()) {
                if (!forceRead) throw new IllegalStateException("Cannot call getMd5 until stream is closed");
                SyncUtil.consumeStream(cin);
            }
            md5 = din.getMessageDigest().digest();
        }
        return md5;
    }

    @Override
    public String getMd5Hex(boolean forceRead) {
        return DatatypeConverter.printHexBinary(getMd5(forceRead));
    }

    protected synchronized void checkLoaded() {
        if (!objectLoaded) {
            loadObject();
            objectLoaded = true;
        }
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", getClass().getSimpleName(), relativePath);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SyncObject that = (SyncObject) o;

        if (!relativePath.equals(that.getRelativePath())) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return relativePath.hashCode();
    }
}

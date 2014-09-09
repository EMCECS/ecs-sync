/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.model;

import com.emc.vipr.sync.util.CountingInputStream;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public abstract class SyncObject<T extends SyncObject<T>> {
    private static final Logger l4j = Logger.getLogger(SyncObject.class);

    protected final String sourceIdentifier;
    protected final String relativePath;
    protected String targetIdentifier;
    protected Set<ObjectAnnotation> annotations = new TreeSet<>();
    protected SyncMetadata metadata;
    private CountingInputStream cin;

    public SyncObject(String sourceIdentifier, String relativePath) {
        if (sourceIdentifier == null) throw new NullPointerException("sourceIdentifier cannot be null");
        this.sourceIdentifier = sourceIdentifier;
        this.relativePath = relativePath;
    }

    /**
     * Implementations should return the raw identifier object of the source system (i.e. ObjectIdentifier for Atmos).
     */
    public abstract Object getRawSourceIdentifier();

    /**
     * Gets the relative path for the object.  If the target is a
     * namespace target, this path will be used when computing the
     * absolute path in the target, relative to the target root.
     */
    public String getRelativePath() {
        return relativePath;
    }

    public abstract boolean hasData();

    public abstract long getSize();

    public final synchronized InputStream getInputStream() {
        if (cin == null) {
            cin = new CountingInputStream(createSourceInputStream());
        }
        return cin;
    }

    /**
     * Must always create a new InputStream and must not return null.
     */
    protected abstract InputStream createSourceInputStream();

    public long getBytesRead() {
        if (cin != null) {
            return cin.getBytesRead();
        } else {
            return 0;
        }
    }

    public abstract boolean hasChildren();

    public String getSourceIdentifier() {
        return sourceIdentifier;
    }

    public String getTargetIdentifier() {
        return targetIdentifier;
    }

    public void setTargetIdentifier(String targetIdentifier) {
        this.targetIdentifier = targetIdentifier;
    }

    public void addAnnotation(ObjectAnnotation annotation) {
        annotations.add(annotation);
    }

    public Set<ObjectAnnotation> getAnnotations() {
        return annotations;
    }

    public <U extends ObjectAnnotation> Set<U> getAnnotations(Class<U> clazz) {
        Set<U> subset = new HashSet<>();
        for (ObjectAnnotation ann : annotations) {
            if (ann.getClass().isAssignableFrom(clazz)) {
                @SuppressWarnings("unchecked") U tann = (U) ann;
                subset.add(tann);
            }
        }
        return subset;
    }

    /**
     * Similar to getAnnotations but it expects only one instance of the class.
     * If not found, it returns null.
     */
    public <U extends ObjectAnnotation> U getAnnotation(Class<U> clazz) {
        Set<U> subset = getAnnotations(clazz);
        if (subset.size() < 1) {
            return null;
        }
        if (subset.size() > 1) {
            l4j.warn("More than one instance of annotation " + clazz + " found!");
        }
        return subset.iterator().next();
    }

    /**
     * @return the metadata
     */
    public SyncMetadata getMetadata() {
        return metadata;
    }

    /**
     * @param metadata the atmosMetadata to set
     */
    public void setMetadata(SyncMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", getClass().getSimpleName(), sourceIdentifier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SyncObject that = (SyncObject) o;

        if (!sourceIdentifier.equals(that.sourceIdentifier)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return sourceIdentifier.hashCode();
    }
}

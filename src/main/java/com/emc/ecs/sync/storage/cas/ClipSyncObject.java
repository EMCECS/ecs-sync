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
package com.emc.ecs.sync.storage.cas;

import com.emc.ecs.sync.config.storage.CasConfig;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.util.PerformanceListener;
import com.emc.ecs.sync.util.ReadOnlyIterator;
import com.emc.object.util.ProgressListener;
import com.filepool.fplibrary.FPLibraryException;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class ClipSyncObject extends SyncObject {
    private CasClip clip;
    private List<EnhancedTag> tags; // lazily-loaded tags
    private int tagIndex = 0;
    private boolean allClipsLoaded = false;
    private ExecutorService blobReadExecutor;
    private ProgressListener progressListener;
    private CasConfig casConfig;
    private String md5Summary;

    public ClipSyncObject(CasStorage source, String clipId, CasClip clip, byte[] cdfData, ObjectMetadata metadata, ExecutorService blobReadExecutor) {
        super(source, clipId, metadata, new ByteArrayInputStream(cdfData), null);
        this.clip = clip;
        this.tags = new ArrayList<>();
        this.blobReadExecutor = blobReadExecutor;
        this.progressListener = source.getOptions().isMonitorPerformance() ? new PerformanceListener(source.getReadWindow()) : null;
        this.casConfig = source.getConfig();
    }

    @Override
    public synchronized long getBytesRead() {
        long total = super.getBytesRead();
        if (tags != null) {
            for (EnhancedTag tag : tags) {
                total += tag.getBytesRead();
            }
        }
        return total;
    }

    /**
     * Note: if forceRead is true, this method will close all tags in the clip
     */
    @Override
    public synchronized String getMd5Hex(boolean forceRead) {
        if (md5Summary == null) {

            // summarize the MD5s of the CDF content and all of the blob-tags
            StringBuilder summary = new StringBuilder("{ CDF: ").append(super.getMd5Hex(forceRead));
            // if we're forcing read, we want to get *all* tags; otherwise, just poll the tags we've already loaded
            for (EnhancedTag tag : (forceRead ? getTags() : tags)) {
                try {
                    if (tag.isBlobAttached()) {
                        summary.append(", tag[").append(tag.getTagNum()).append("]: ");
                        summary.append(DatatypeConverter.printHexBinary(tag.getMd5Digest(forceRead)));
                    }
                } finally {
                    if (forceRead) tag.close();
                }
            }
            summary.append(" }");
            md5Summary = summary.toString();
        }
        return md5Summary;
    }

    private synchronized boolean loadNextTag() {
        if (allClipsLoaded) return false; // we already have all the tags

        // actually pull next tag from clip
        CasTag tag = null;
        try {
            tag = clip.FetchNext();
            if (tag == null) {
                // the tag before this was the last tag
                allClipsLoaded = true;
                return false;
            }
            EnhancedTag enhancedTag = new EnhancedTag(tag, tagIndex++, getSource().getOptions().getBufferSize(), progressListener, blobReadExecutor);
            enhancedTag.setDrainOnError(casConfig.isDrainBlobsOnError());
            tags.add(enhancedTag);
            return true;
        } catch (Throwable t) {
            if (tag != null) tag.close();
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            else if (t instanceof FPLibraryException)
                throw new RuntimeException(CasStorage.summarizeError((FPLibraryException) t), t);
            else throw new RuntimeException(t);
        }
    }

    @Override
    public synchronized void close() {
        if (tags != null) {
            for (EnhancedTag tag : tags) {
                tag.close();
            }
        }
        if (clip != null) {
            clip.close();
            clip = null;
        }
    }

    public CasClip getClip() {
        return clip;
    }

    public Iterable<EnhancedTag> getTags() {
        return new Iterable<EnhancedTag>() {
            @Override
            public Iterator<EnhancedTag> iterator() {
                return new ReadOnlyIterator<EnhancedTag>() {
                    int nextClipIdx = 0;

                    @Override
                    protected EnhancedTag getNextObject() {
                        synchronized (ClipSyncObject.this) {
                            // if we're at the end of the local cache, try to load another tag
                            if (nextClipIdx >= tags.size() && !loadNextTag()) return null;
                            return tags.get(nextClipIdx++);
                        }
                    }
                };
            }
        };
    }
}

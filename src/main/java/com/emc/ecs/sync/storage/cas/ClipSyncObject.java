/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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

import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.PerformanceListener;
import com.emc.ecs.sync.util.ReadOnlyIterator;
import com.emc.object.util.ProgressListener;
import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPLibraryException;
import com.filepool.fplibrary.FPTag;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class ClipSyncObject extends SyncObject {
    private FPClip clip;
    private List<ClipTag> tags; // lazily-loaded tags
    private int clipIndex = 0;
    private boolean allClipsLoaded = false;
    private ExecutorService blobReadExecutor;
    private ProgressListener progressListener;
    private String md5Summary;

    public ClipSyncObject(SyncStorage source, String clipId, FPClip clip, byte[] cdfData, ObjectMetadata metadata, ExecutorService blobReadExecutor) {
        super(source, clipId, metadata, new ByteArrayInputStream(cdfData), null);
        this.clip = clip;
        this.tags = new ArrayList<>();
        this.blobReadExecutor = blobReadExecutor;
        progressListener = source.getOptions().isMonitorPerformance() ? new PerformanceListener(source.getReadWindow()) : null;
    }

    @Override
    public synchronized long getBytesRead() {
        long total = super.getBytesRead();
        if (tags != null) {
            for (ClipTag tag : tags) {
                total += tag.getBytesRead();
            }
        }
        return total;
    }

    @Override
    public synchronized String getMd5Hex(boolean forceRead) {
        if (md5Summary == null) {

            // summarize the MD5s of the CDF content and all of the blob-tags
            StringBuilder summary = new StringBuilder("{ CDF: ").append(super.getMd5Hex(forceRead));
            // if we're forcing read, we want to get *all* tags; otherwise, just poll the tags we've already loaded
            for (ClipTag tag : (forceRead ? getTags() : tags)) {
                if (tag.isBlobAttached()) {
                    summary.append(", tag[").append(tag.getTagNum()).append("]: ");
                    summary.append(DatatypeConverter.printHexBinary(tag.getMd5Digest(forceRead)));
                }
            }
            summary.append(" }");
            md5Summary = summary.toString();
        }
        return md5Summary;
    }

    private synchronized boolean loadNextTag() {
        if (allClipsLoaded) return false; // we already have all the tags
        FPTag tag = null;
        try {
            // actually pull next tag from clip
            tag = clip.FetchNext();
            if (tag == null) {
                // the tag before this was the last tag
                allClipsLoaded = true;
                return false;
            }
            ClipTag clipTag = new ClipTag(tag, clipIndex++, getSource().getOptions().getBufferSize(), progressListener, blobReadExecutor);
            tags.add(clipTag);
            return true;
        } catch (Throwable t) {
            CasStorage.safeClose(tag, getRelativePath(), clipIndex);
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            else if (t instanceof FPLibraryException)
                throw new RuntimeException(CasStorage.summarizeError((FPLibraryException) t), t);
            else throw new RuntimeException(t);
        }
    }

    @Override
    public synchronized void close() {
        if (tags != null) {
            for (ClipTag tag : tags) {
                CasStorage.safeClose(tag, getRelativePath());
            }
        }

        CasStorage.safeClose(clip, getRelativePath());
        clip = null;
    }

    public FPClip getClip() {
        return clip;
    }

    public Iterable<ClipTag> getTags() {
        return new Iterable<ClipTag>() {
            @Override
            public Iterator<ClipTag> iterator() {
                return new ReadOnlyIterator<ClipTag>() {
                    int nextClipIdx = 0;

                    @Override
                    protected ClipTag getNextObject() {
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

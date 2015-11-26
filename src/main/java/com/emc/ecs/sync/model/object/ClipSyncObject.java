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
import com.emc.ecs.sync.util.CasUtil;
import com.emc.ecs.sync.util.ClipTag;
import com.emc.ecs.sync.util.TimingUtil;
import com.emc.object.util.ProgressListener;
import com.filepool.fplibrary.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

public class ClipSyncObject extends AbstractSyncObject<String> {
    private static final Logger log = LoggerFactory.getLogger(ClipSyncObject.class);

    protected SyncPlugin parentPlugin;
    protected FPPool pool;
    protected FPClip clip;
    protected String clipName;
    protected byte[] cdfData;
    protected List<ClipTag> tags;
    protected String md5Summary;

    public ClipSyncObject(SyncPlugin parentPlugin, FPPool pool, String clipId, String relativePath) {
        super(clipId, clipId, relativePath, false);
        this.parentPlugin = parentPlugin;
        this.pool = pool;
    }

    @Override
    public InputStream createSourceInputStream() {
        checkLoaded();
        return new ByteArrayInputStream(cdfData);
    }

    @Override
    protected void loadObject() {
        int tagCount = 0;
        FPTag tag = null;
        try {

            // open the clip
            clip = TimingUtil.time(parentPlugin, CasUtil.OPERATION_OPEN_CLIP, new Callable<FPClip>() {
                @Override
                public FPClip call() throws Exception {
                    return new FPClip(pool, rawSourceIdentifier, FPLibraryConstants.FP_OPEN_FLAT);
                }
            });

            // pull the CDF
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            TimingUtil.time(parentPlugin, CasUtil.OPERATION_READ_CDF, new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    clip.RawRead(baos);
                    return null;
                }
            });

            clipName = clip.getName();
            cdfData = baos.toByteArray();
            // Track that we read data
            if (parentPlugin.isMonitorPerformance()) {
                parentPlugin.getReadPerformanceCounter().increment(baos.size());
            }

            metadata = new SyncMetadata();
            metadata.setContentLength(clip.getTotalSize());
            metadata.setModificationTime(new Date(clip.getCreationDate()));

            // pull all clip tags
            tags = new ArrayList<>();
            ProgressListener listener = parentPlugin.isMonitorPerformance() ? new ClipProgress() : null;
            while ((tag = clip.FetchNext()) != null) {
                tags.add(new ClipTag(tag, tagCount++, parentPlugin.getBufferSize(), listener));
            }
        } catch (Exception e) {
            if (tag != null) {
                try {
                    tag.Close();
                } catch (FPLibraryException e2) {
                    log.warn("could not close tag {}.{}: {}", rawSourceIdentifier, tagCount, CasUtil.summarizeError(e2));
                }
            }

            if (e instanceof FPLibraryException)
                throw new RuntimeException(CasUtil.summarizeError((FPLibraryException) e), e);
            else throw new RuntimeException(e);
        }
    }

    @Override
    public long getBytesRead() {
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
            if (forceRead) checkLoaded();
            if (cdfData == null) throw new UnsupportedOperationException("CDF is not loaded");

            // summarize the MD5s of the CDF content and all of the blob-tags
            StringBuilder summary = new StringBuilder("{ CDF: ").append(super.getMd5Hex(forceRead));
            for (ClipTag tag : tags) {
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

    @Override
    public void close() {
        if (tags != null) {
            for (ClipTag tag : tags) {
                try {
                    tag.close();
                } catch (Throwable t) {
                    log.warn("could not close tag {}.{}: {}", rawSourceIdentifier, tag.getTagNum(), t);
                }
            }
        }

        if (clip != null) {
            try {
                clip.Close();
            } catch (Throwable t) {
                log.warn("could not close clip {}: {}", rawSourceIdentifier, t);
            } finally {
                clip = null;
            }
        }
    }

    @Override
    public void reset() {
        close();
        tags = null;
        cdfData = null;
        md5Summary = null;
        super.reset();
    }

    public String getClipName() {
        return clipName;
    }

    public byte[] getCdfData() {
        return cdfData;
    }

    public List<ClipTag> getTags() {
        return tags;
    }

    private class ClipProgress implements ProgressListener {
        @Override
        public void progress(long completed, long total) {

        }

        @Override
        public void transferred(long size) {
            if(parentPlugin.getReadPerformanceCounter() != null) {
                parentPlugin.getReadPerformanceCounter().increment(size);
            }
        }
    }
}

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
import com.emc.ecs.sync.util.TransferProgressListener;
import com.emc.object.util.ProgressListener;
import com.filepool.fplibrary.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.InputStream;
import java.util.Date;
import java.util.concurrent.Callable;

public class SimpleClipSyncObject extends AbstractSyncObject<String> {
    private static final Logger log = LoggerFactory.getLogger(SimpleClipSyncObject.class);

    protected FPPool pool;
    protected FPClip clip;
    protected String clipName;
    protected ClipTag dataTag;

    public SimpleClipSyncObject(SyncPlugin parentPlugin, FPPool pool, String clipId, String relativePath) {
        super(parentPlugin, clipId, clipId, relativePath, false);
        this.pool = pool;
    }

    @Override
    public InputStream createSourceInputStream() {
        throw new UnsupportedOperationException("streaming mode not currently supported");
    }

    @Override
    protected void loadObject() {

        FPTag tag_data = null;
        FPTag tag_user = null;
        try {

            // open the clip
            clip = TimingUtil.time(getParentPlugin(), CasUtil.OPERATION_OPEN_CLIP, new Callable<FPClip>() {
                @Override
                public FPClip call() throws Exception {
                    return new FPClip(pool, getRawSourceIdentifier(), FPLibraryConstants.FP_OPEN_FLAT);
                }
            });

            clipName = clip.getName();

            // system metadata
            tag_data = clip.getTopTag();
            metadata = new SyncMetadata();
            metadata.setContentLength(clip.getTotalSize());
            metadata.setContentType(tag_data.getStringAttribute("Content-Type"));
            String theDate = tag_data.getStringAttribute("x-emc-sync-mtime");
            metadata.setModificationTime(new Date(Long.parseLong(theDate)));

            String tagName = tag_data.getTagName();

            // user metadata
            try {
                while ((tag_user = clip.FetchNext()) != null) {
                    if (!tag_user.getTagName().equals(tagName)) {
                        metadata.setUserMetadataValue(tag_user.getStringAttribute(("name")), tag_user.getStringAttribute("value"));
                        tag_user.Close();
                    }
                }
            } catch(Exception e) {
                throw e;
            } finally {
                if (tag_user != null) {
                    try {
                        tag_user.Close();
                    } catch (FPLibraryException e2) {
                        log.warn("could not close tag_user {}: {}", getRawSourceIdentifier(), CasUtil.summarizeError(e2));
                    }
                }
            }

            ProgressListener listener = null;
            if (getParentPlugin().isMonitorPerformance()) {
                listener = new TransferProgressListener(getParentPlugin().getReadPerformanceCounter());
            }
            dataTag = new ClipTag(tag_data, 0, getParentPlugin().getBufferSize(), listener);
        } catch (Exception e) {
            if (tag_data != null) {
                try {
                    tag_data.Close();
                } catch (FPLibraryException e2) {
                    log.warn("could not close tag_data {}: {}", getRawSourceIdentifier(), CasUtil.summarizeError(e2));
                }
            }
            if (e instanceof FPLibraryException)
                throw new RuntimeException(CasUtil.summarizeError((FPLibraryException) e), e);
            else throw new RuntimeException(e);
        }
    }

    @Override
    public String getMd5Hex(boolean forceRead) {
        checkLoaded();
        return DatatypeConverter.printHexBinary(dataTag.getMd5Digest(forceRead));
    }

    public void delete() {
        throw new UnsupportedOperationException("deleting the clip is not currently supported");
    }

    @Override
    public void close() {
        if (dataTag != null) {
            try {
                dataTag.close();
            } catch (Throwable t) {
                log.warn("could not close dataTag {}: {}", getRawSourceIdentifier(), t);
            } finally {
                dataTag = null;
            }
        }
        if (clip != null) {
            try {
                clip.Close();
            } catch (Throwable t) {
                log.warn("could not close clip {}: {}", getRawSourceIdentifier(), t);
            } finally {
                clip = null;
            }
        }
    }
}

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
import com.filepool.fplibrary.FPClip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.util.List;

public class ClipSyncObject extends SyncObject {
    private static final Logger log = LoggerFactory.getLogger(ClipSyncObject.class);

    private FPClip clip;
    private List<ClipTag> tags;
    private String md5Summary;

    public ClipSyncObject(SyncStorage source, String clipId, FPClip clip, byte[] cdfData, List<ClipTag> tags, ObjectMetadata metadata) {
        super(source, clipId, metadata, new ByteArrayInputStream(cdfData), null);
        this.clip = clip;
        this.tags = tags;
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
                    log.warn("could not close tag {}.{}: {}", getRelativePath(), tag.getTagNum(), t);
                }
            }
        }

        if (clip != null) {
            try {
                clip.Close();
            } catch (Throwable t) {
                log.warn("could not close clip {}: {}", getRelativePath(), t);
            } finally {
                clip = null;
            }
        }
    }

    public FPClip getClip() {
        return clip;
    }

    public List<ClipTag> getTags() {
        return tags;
    }
}

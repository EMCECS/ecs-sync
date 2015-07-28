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

import com.emc.vipr.sync.util.ClipTag;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

public class ClipSyncObject extends AbstractSyncObject<String> {
    private String clipName;
    private byte[] cdfData;
    private List<ClipTag> tags;

    public ClipSyncObject(String clipId, String relativePath) {
        super(clipId, clipId, relativePath, false);
    }

    @Override
    public InputStream createSourceInputStream() {
        return new ByteArrayInputStream(cdfData);
    }

    @Override
    protected void loadObject() {
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

    public String getClipName() {
        return clipName;
    }

    public void setClipName(String clipName) {
        this.clipName = clipName;
    }

    public byte[] getCdfData() {
        return cdfData;
    }

    public void setCdfData(byte[] cdfData) {
        this.cdfData = cdfData;
    }

    public List<ClipTag> getTags() {
        return tags;
    }

    public void setTags(List<ClipTag> tags) {
        this.tags = tags;
    }
}

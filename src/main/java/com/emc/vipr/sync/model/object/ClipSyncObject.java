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

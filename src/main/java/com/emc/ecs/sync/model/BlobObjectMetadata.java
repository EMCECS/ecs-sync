package com.emc.ecs.sync.model;

public class BlobObjectMetadata extends ObjectMetadata{

    private long blobObjectLength;

    public long getBlobObjectLength() {
        return blobObjectLength;
    }

    public void setBlobObjectLength(long blobObjectLength) {
        this.blobObjectLength = blobObjectLength;
    }
}

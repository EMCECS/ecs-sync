package com.emc.ecs.sync.model.object;

import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.model.SyncMetadata;

import java.io.IOException;

/**
 * Base class for SyncObjects that will simply wrap the source stream in some sort of other InputStream like
 * FilterInputStreams.
 */
public abstract class WrappedSyncObject implements SyncObject {
    protected SyncObject parent;

    public WrappedSyncObject(SyncObject parent) {
        this.parent = parent;
    }

    @Override
    public SyncPlugin getParentPlugin() {
        return parent.getParentPlugin();
    }

    @Override
    public Object getRawSourceIdentifier() {
        return parent.getRawSourceIdentifier();
    }

    @Override
    public String getSourceIdentifier() {
        return parent.getSourceIdentifier();
    }

    @Override
    public String getRelativePath() {
        return parent.getRelativePath();
    }

    @Override
    public boolean isDirectory() {
        return parent.isDirectory();
    }

    @Override
    public boolean isLargeObject(int threshold) {
        return parent.isLargeObject(threshold);
    }

    @Override
    public String getTargetIdentifier() {
        return parent.getTargetIdentifier();
    }

    @Override
    public SyncMetadata getMetadata() {
        return parent.getMetadata();
    }

    @Override
    public boolean requiresPostStreamMetadataUpdate() {
        return parent.requiresPostStreamMetadataUpdate();
    }

    @Override
    public void setTargetIdentifier(String targetIdentifier) {
        parent.setTargetIdentifier(targetIdentifier);
    }

    @Override
    public void setMetadata(SyncMetadata metadata) {
        parent.setMetadata(metadata);
    }

    @Override
    public long getBytesRead() {
        return parent.getBytesRead();
    }

    @Override
    public String getMd5Hex(boolean forceRead) {
        return parent.getMd5Hex(forceRead);
    }

    @Override
    public void incFailureCount() {
        parent.incFailureCount();
    }

    @Override
    public int getFailureCount() {
        return parent.getFailureCount();
    }

    @Override
    public void reset() {
        parent.reset();
    }

    @Override
    public void close() throws IOException {
        parent.close();
    }
}

package com.emc.ecs.sync.util;

import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.SyncObject;

import java.io.IOException;
import java.io.InputStream;

public abstract class DelegatingSyncObject<I> implements SyncObject<I> {
    protected SyncObject<I> delegate;

    public DelegatingSyncObject(SyncObject<I> delegate) {
        this.delegate = delegate;
    }

    @Override
    public SyncPlugin getParentPlugin() {
        return delegate.getParentPlugin();
    }

    @Override
    public I getRawSourceIdentifier() {
        return delegate.getRawSourceIdentifier();
    }

    @Override
    public String getSourceIdentifier() {
        return delegate.getSourceIdentifier();
    }

    @Override
    public String getRelativePath() {
        return delegate.getRelativePath();
    }

    @Override
    public boolean isDirectory() {
        return delegate.isDirectory();
    }

    @Override
    public boolean isLargeObject(int threshold) {
        return delegate.isLargeObject(threshold);
    }

    @Override
    public String getTargetIdentifier() {
        return delegate.getTargetIdentifier();
    }

    @Override
    public SyncMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public boolean requiresPostStreamMetadataUpdate() {
        return delegate.requiresPostStreamMetadataUpdate();
    }

    @Override
    public void setTargetIdentifier(String targetIdentifier) {
        delegate.setTargetIdentifier(targetIdentifier);
    }

    @Override
    public void setMetadata(SyncMetadata metadata) {
        delegate.setMetadata(metadata);
    }

    @Override
    public InputStream getInputStream() {
        return delegate.getInputStream();
    }

    @Override
    public long getBytesRead() {
        return delegate.getBytesRead();
    }

    @Override
    public String getMd5Hex(boolean forceRead) {
        return delegate.getMd5Hex(forceRead);
    }

    @Override
    public void incFailureCount() {
        delegate.incFailureCount();
    }

    @Override
    public int getFailureCount() {
        return delegate.getFailureCount();
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}

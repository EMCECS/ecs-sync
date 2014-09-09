package com.emc.vipr.sync.test.util;

import com.emc.vipr.sync.model.SyncObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

public class TestSyncObject extends SyncObject<TestSyncObject> {
    private byte[] data;
    private List<TestSyncObject> children;

    public TestSyncObject(String identifier, String relativePath, byte[] data, List<TestSyncObject> children) {
        super(identifier, relativePath);
        this.data = data;
        this.children = children;
    }

    @Override
    public Object getRawSourceIdentifier() {
        return sourceIdentifier;
    }

    @Override
    public boolean hasData() {
        return data != null;
    }

    @Override
    public long getSize() {
        return (data == null) ? 0 : data.length;
    }

    @Override
    protected InputStream createSourceInputStream() {
        return (data == null) ? null : new ByteArrayInputStream(data);
    }

    @Override
    public boolean hasChildren() {
        return children != null;
    }

    public byte[] getData() {
        return data;
    }

    public List<TestSyncObject> getChildren() {
        return children;
    }
}

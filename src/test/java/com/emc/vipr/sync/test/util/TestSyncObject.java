package com.emc.vipr.sync.test.util;

import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

public class TestSyncObject extends SyncObject<String> {
    private byte[] data;
    private List<TestSyncObject> children;

    public TestSyncObject(String identifier, String relativePath, byte[] data, List<TestSyncObject> children) {
        super(identifier, identifier, relativePath, children != null);
        this.data = data;
        this.children = children;
    }

    @Override
    protected InputStream createSourceInputStream() {
        return (data == null) ? null : new ByteArrayInputStream(data);
    }

    @Override
    protected void loadObject() {
        if (metadata == null) metadata = new SyncMetadata();
        if (data != null) metadata.setSize(data.length);
        else metadata.setSize(0);
    }

    public byte[] getData() {
        return data;
    }

    public List<TestSyncObject> getChildren() {
        return children;
    }
}

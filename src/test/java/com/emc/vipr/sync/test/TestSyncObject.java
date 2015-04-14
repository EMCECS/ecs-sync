package com.emc.vipr.sync.test;

import com.emc.vipr.sync.model.SyncAcl;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.object.AbstractSyncObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestSyncObject extends AbstractSyncObject<String> {
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

    /**
     * For cases when you don't want the sync to modify the original objects (perhaps you're comparing them to the
     * result of a sync)
     */
    public TestSyncObject deepCopy() throws CloneNotSupportedException {
        TestSyncObject copy = new TestSyncObject(sourceIdentifier, relativePath,
                (data == null ? null : Arrays.copyOf(data, data.length)),
                copyChildren());
        copy.setMetadata(copyMetadata());
        return copy;
    }

    protected List<TestSyncObject> copyChildren() throws CloneNotSupportedException {
        if (children == null) return null;
        List<TestSyncObject> copiedChildren = new ArrayList<TestSyncObject>();
        for (TestSyncObject child : children) {
            copiedChildren.add(child.deepCopy());
        }
        return copiedChildren;
    }

    protected SyncMetadata copyMetadata() throws CloneNotSupportedException {
        SyncMetadata metadata = getMetadata();
        SyncMetadata metaCopy = new SyncMetadata();
        if (metadata.getAcl() != null) metaCopy.setAcl((SyncAcl) metadata.getAcl().clone());
        metaCopy.setChecksum(metadata.getChecksum()); // might be hard to duplicate a running checksum
        metaCopy.setContentType(metadata.getContentType());
        metaCopy.setExpirationDate(metadata.getExpirationDate());
        metaCopy.setModificationTime(metadata.getModificationTime());
        metaCopy.setSize(metadata.getSize());
        for (String key : metadata.getUserMetadata().keySet()) {
            SyncMetadata.UserMetadata um = metadata.getUserMetadata().get(key);
            metaCopy.setUserMetadataValue(key, um.getValue(), um.isIndexed());
        }
        return metaCopy;
    }
}

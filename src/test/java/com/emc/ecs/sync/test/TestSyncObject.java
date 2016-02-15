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
package com.emc.ecs.sync.test;

import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.model.SyncAcl;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.AbstractSyncObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestSyncObject extends AbstractSyncObject<String> {
    private byte[] data;
    private List<TestSyncObject> children;

    public TestSyncObject(SyncPlugin parentPlugin, String identifier, String relativePath, byte[] data,
                          List<TestSyncObject> children) {
        super(parentPlugin, identifier, identifier, relativePath, children != null);
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
        if (data != null) metadata.setContentLength(data.length);
        else metadata.setContentLength(0);
    }

    /**
     * Don't lose the test metadata on reset
     */
    @Override
    public void reset() {
        SyncMetadata meta = getMetadata();
        super.reset();
        setMetadata(meta);
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
        TestSyncObject copy = new TestSyncObject(getParentPlugin(), getSourceIdentifier(), getRelativePath(),
                (data == null ? null : Arrays.copyOf(data, data.length)),
                copyChildren());
        copy.setMetadata(copyMetadata());
        return copy;
    }

    protected List<TestSyncObject> copyChildren() throws CloneNotSupportedException {
        if (children == null) return null;
        List<TestSyncObject> copiedChildren = new ArrayList<>();
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
        metaCopy.setContentLength(metadata.getContentLength());
        for (String key : metadata.getUserMetadata().keySet()) {
            SyncMetadata.UserMetadata um = metadata.getUserMetadata().get(key);
            metaCopy.setUserMetadataValue(key, um.getValue(), um.isIndexed());
        }
        return metaCopy;
    }
}

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

import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.SyncUtil;
import com.emc.util.StreamUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.*;

public class TestObjectTarget extends SyncTarget {
    private Map<String, List<TestSyncObject>> childrenMap =
            Collections.synchronizedMap(new HashMap<String, List<TestSyncObject>>());

    @Override
    public void filter(SyncObject obj) {
        try {
            if (obj.getRelativePath().isEmpty())
                return; // including the root directory will throw off the tests

            obj.setTargetIdentifier(obj.getRelativePath());

            byte[] data = !obj.isDirectory() ? StreamUtil.readAsBytes(obj.getInputStream()) : null;
            TestSyncObject testObject = new TestSyncObject(this, obj.getSourceIdentifier(), obj.getRelativePath(),
                    data, obj.isDirectory() ? new ArrayList<TestSyncObject>() : null);

            // copy metadata
            testObject.setMetadata(obj.getMetadata()); // making this simple

            // equivalent of mkdirs()
            mkdirs(obj.getRelativePath());

            // add to parent (root objects will be added to "")
            String parentPath = SyncUtil.parentPath(obj.getRelativePath());
            if (parentPath == null) parentPath = "";
            addChild(parentPath, testObject);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        obj.setTargetIdentifier(obj.getRelativePath());
        return getObject(obj.getRelativePath());
    }

    @Override
    public void cleanup() {
        super.cleanup();
        synchronized (getChildren("")) {
            for (TestSyncObject object : getChildren("")) {
                linkChildren(object);
            }
        }
    }

    @Override
    public boolean canHandleTarget(String targetUri) {
        return false;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getDocumentation() {
        return null;
    }

    @Override
    public Options getCustomOptions() {
        return null;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
    }

    public void ingest(List<TestSyncObject> objects) {
        List<TestSyncObject> clones = new ArrayList<>();
        for (TestSyncObject object : objects) {
            try {
                clones.add(object.deepCopy());
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException("deep copy failed", e);
            }
        }
        recursiveIngest(clones);
    }

    private void recursiveIngest(List<TestSyncObject> objects) {
        for (TestSyncObject object : objects) {
            filter(object);
            if (object.isDirectory()) recursiveIngest(object.getChildren());
        }
    }

    public List<TestSyncObject> getRootObjects() {
        return getChildren("");
    }

    private void mkdirs(String path) {
        String parent = SyncUtil.parentPath(path);
        if (parent == null) return;
        mkdirs(parent);
        if (getObject(parent) == null) {
            // add directory
            String grandparent = SyncUtil.parentPath(parent);
            if (grandparent == null) grandparent = "";
            addChild(grandparent, new TestSyncObject(this, parent, parent, null, new ArrayList<TestSyncObject>()));
        }
    }

    private synchronized TestSyncObject getObject(String relativePath) {
        String parentPath = SyncUtil.parentPath(relativePath);
        if (parentPath == null) parentPath = "";
        for (TestSyncObject child : getChildren(parentPath)) {
            if (child.getRelativePath().equals(relativePath)) return child;
        }
        return null;
    }

    private synchronized List<TestSyncObject> getChildren(String relativePath) {
        List<TestSyncObject> children = childrenMap.get(relativePath);
        if (children == null) {
            children = Collections.synchronizedList(new ArrayList<TestSyncObject>());
            childrenMap.put(relativePath, children);
        }
        return children;
    }

    private synchronized void addChild(String parentPath, TestSyncObject object) {
        List<TestSyncObject> children = getChildren(parentPath);
        children.remove(object); // in case mkdirs already created a directory that is now being sync'd
        children.add(object);
    }

    private synchronized void linkChildren(TestSyncObject object) {
        if (object.isDirectory()) {
            for (TestSyncObject child : getChildren(object.getRelativePath())) {
                object.getChildren().remove(child); // in case linking is done multiple times
                object.getChildren().add(child);
                linkChildren(child);
            }
        }
    }
}

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
package com.emc.vipr.sync.test;

import com.emc.util.StreamUtil;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.object.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.File;
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

            // for these tests to be valid, we have to normalize relative paths. S3 for example will append a slash to
            // prefix results in a list operation, so we have to remove those here. using the java.io.File abstraction
            // should work well
            File relativePath = new File(obj.getRelativePath());

            byte[] data = !obj.isDirectory() ? StreamUtil.readAsBytes(obj.getInputStream()) : null;
            TestSyncObject testObject = new TestSyncObject(obj.getSourceIdentifier(), relativePath.getPath(),
                    data, obj.isDirectory() ? new ArrayList<TestSyncObject>() : null);

            // copy metadata
            testObject.setMetadata(obj.getMetadata()); // making this simple

            // equivalent of mkdirs()
            mkdirs(relativePath);

            // add to parent (root objects will be added to "")
            String parentPath = relativePath.getParent();
            if (parentPath == null) parentPath = "";
            List<TestSyncObject> children = getChildren(parentPath);
            synchronized (children) {
                children.add(testObject);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
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

    public void recursiveIngest(List<TestSyncObject> testObjects) {
        for (TestSyncObject object : testObjects) {
            filter(object);
            if (object.isDirectory()) {
                recursiveIngest(object.getChildren());
            }
        }
    }

    public List<TestSyncObject> getRootObjects() {
        return getChildren("");
    }

    private void mkdirs(File path) {
        File parent = path.getParentFile();
        if (parent == null) return;
        mkdirs(parent);
        if (getObject(parent.getPath()) == null) {
            // add directory
            String parentParent = parent.getParent();
            if (parentParent == null) parentParent = "";
            List<TestSyncObject> children = getChildren(parentParent);
            synchronized (children) {
                children.add(new TestSyncObject(parent.getPath(), parent.getPath(), null, new ArrayList<TestSyncObject>()));
            }
        }
    }

    private TestSyncObject getObject(String relativePath) {
        String parentPath = new File(relativePath).getParent();
        if (parentPath == null) parentPath = "";
        List<TestSyncObject> children = getChildren(parentPath);
        synchronized (children) {
            for (TestSyncObject child : children) {
                if (child.getRelativePath().equals(relativePath)) return child;
            }
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

    private void linkChildren(TestSyncObject object) {
        if (object.isDirectory()) {
            List<TestSyncObject> children = getChildren(object.getRelativePath());
            synchronized (children) {
                for (TestSyncObject child : children) {
                    object.getChildren().add(child);
                    linkChildren(child);
                }
            }
        }
    }
}

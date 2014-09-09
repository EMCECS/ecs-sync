package com.emc.vipr.sync.test.util;

import com.emc.util.StreamUtil;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SyncObject;
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
    public void filter(SyncObject<?> obj) {
        try {
            if (obj.getRelativePath().isEmpty())
                return; // including the root directory will throw off the tests

            byte[] data = obj.hasData() ? StreamUtil.readAsBytes(obj.getInputStream()) : null;
            TestSyncObject testObject = new TestSyncObject(obj.getSourceIdentifier(), obj.getRelativePath(),
                    data, obj.hasChildren() ? new ArrayList<TestSyncObject>() : null);

            // add to parent (root objects will be added to "")
            String parentPath = new File(testObject.getRelativePath()).getParent();
            if (parentPath == null) parentPath = "";
            getChildren(parentPath).add(testObject);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        for (TestSyncObject object : getChildren("")) {
            linkChildren(object);
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

    public List<TestSyncObject> getRootObjects() {
        return getChildren("");
    }

    private synchronized List<TestSyncObject> getChildren(String relativePath) {
        List<TestSyncObject> children = childrenMap.get(relativePath);
        if (children == null) {
            children = new ArrayList<>();
            childrenMap.put(relativePath, children);
        }
        return children;
    }

    private void linkChildren(TestSyncObject object) {
        if (object.hasChildren()) {
            for (TestSyncObject child : getChildren(object.getRelativePath())) {
                object.getChildren().add(child);
                linkChildren(child);
            }
        }
    }
}

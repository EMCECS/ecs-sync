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

            // for these tests to be valid, we have to normalize relative paths. S3 for example will append a slash to
            // prefix results in a list operation, so we have to remove those here. using the java.io.File abstraction
            // should work well
            File relativePath = new File(obj.getRelativePath());

            byte[] data = !obj.isDirectory() ? StreamUtil.readAsBytes(obj.getInputStream()) : null;
            TestSyncObject testObject = new TestSyncObject(obj.getSourceIdentifier(), relativePath.getPath(),
                    data, obj.isDirectory() ? new ArrayList<TestSyncObject>() : null);

            // copy metadata
            testObject.setMetadata(obj.getMetadata()); // making this simple

            // add to parent (root objects will be added to "")
            String parentPath = relativePath.getParent();
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
            children = Collections.synchronizedList(new ArrayList<TestSyncObject>());
            childrenMap.put(relativePath, children);
        }
        return children;
    }

    private void linkChildren(TestSyncObject object) {
        if (object.isDirectory()) {
            for (TestSyncObject child : getChildren(object.getRelativePath())) {
                object.getChildren().add(child);
                linkChildren(child);
            }
        }
    }
}

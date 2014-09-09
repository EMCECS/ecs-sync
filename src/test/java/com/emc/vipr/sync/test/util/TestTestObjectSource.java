package com.emc.vipr.sync.test.util;


import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestTestObjectSource {
    private static final int MAX_SIZE = 10240; // 10k
    private static final int NUM_OBJECTS = 1000;

    @Test
    public void testRandomObjectGeneration() {
        List<TestSyncObject> objects = TestObjectSource.generateRandomObjects(NUM_OBJECTS, MAX_SIZE);

        Assert.assertNotNull("list is null", objects);
        Assert.assertEquals("list is wrong size", NUM_OBJECTS, objects.size());

        // loop through all objects.. make sure some have children and some have data and that data is never above
        // threshold
        VerificationResults results = new VerificationResults();
        verify(objects, results);

        Assert.assertTrue("max size exceeded", results.maxSize < MAX_SIZE);
        Assert.assertTrue("no children detected", results.hasChildren);
        Assert.assertTrue("no data detected", results.hasData);

        System.out.println(String.format("total objects: %s, dirs: %s, data objects: %s, total size: %s",
                results.totalObjects, results.totalDirs, results.totalDataObjects, results.totalSize));
    }

    private void verify(List<TestSyncObject> objects, VerificationResults results) {
        for (TestSyncObject object : objects) {
            results.totalObjects++;
            if (object.hasData()) {
                int objectSize = (int) object.getSize();
                results.hasData = true;
                results.totalDataObjects++;
                results.totalSize += objectSize;
                if (objectSize > results.maxSize) results.maxSize = objectSize;
            }
            if (object.hasChildren()) {
                results.totalDirs++;
                results.hasChildren = true;
                verify(object.getChildren(), results);
            }
        }
    }

    private class VerificationResults {
        public boolean hasChildren = false, hasData = false;
        public int maxSize = 0, totalObjects = 0, totalDataObjects = 0, totalDirs = 0, totalSize = 0;
    }
}

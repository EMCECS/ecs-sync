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


import com.emc.vipr.sync.model.object.S3SyncObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestObjectSourceTest {
    private static final int MAX_SIZE = 10240; // 10k
    private static final int NUM_OBJECTS = 1000;

    @Test
    public void testRandomObjectGeneration() {
        TestObjectSource testSource = new TestObjectSource(NUM_OBJECTS, MAX_SIZE, null) {
            @Override
            public void delete(S3SyncObject syncObject) {

            }
        };
        List<TestSyncObject> objects = testSource.getObjects();

        Assert.assertNotNull("list is null", objects);
        Assert.assertEquals("list is wrong size", NUM_OBJECTS, objects.size());

        // loop through all objects.. make sure some have children and some have data and that data is never above
        // threshold
        VerificationResults results = new VerificationResults();
        verify(objects, results);

        Assert.assertTrue("max size exceeded", results.maxSize < MAX_SIZE);
        Assert.assertTrue("no directories detected", results.hasDirectories);
        Assert.assertTrue("no data detected", results.hasData);

        System.out.println(String.format("total objects: %s, dirs: %s, data objects: %s, total size: %s",
                results.totalObjects, results.totalDirs, results.totalDataObjects, results.totalSize));
    }

    private void verify(List<TestSyncObject> objects, VerificationResults results) {
        for (TestSyncObject object : objects) {
            results.totalObjects++;
            if (object.isDirectory()) {
                results.totalDirs++;
                results.hasDirectories = true;
                verify(object.getChildren(), results);
            } else {
                int objectSize = (int) object.getMetadata().getSize();
                results.hasData = true;
                results.totalDataObjects++;
                results.totalSize += objectSize;
                if (objectSize > results.maxSize) results.maxSize = objectSize;
            }
        }
    }

    private class VerificationResults {
        public boolean hasDirectories = false, hasData = false;
        public int maxSize = 0, totalObjects = 0, totalDataObjects = 0, totalDirs = 0, totalSize = 0;
    }
}

/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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


import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

public class TestStorageTest {
    private static final int MAX_SIZE = 10240; // 10k
    private static final int NUM_OBJECTS = 1000;

    @Test
    public void testRandomObjectGeneration() {
        TestConfig testConfig = new TestConfig();
        testConfig.setObjectCount(NUM_OBJECTS);
        testConfig.setMaxSize(MAX_SIZE);
        testConfig.setDiscardData(false);
        TestStorage testSource = new TestStorage();
        testSource.setConfig(testConfig);
        testSource.configure(testSource, null, null);

        List<? extends SyncObject> objects = testSource.getRootObjects();

        Assertions.assertNotNull(objects, "list is null");
        Assertions.assertEquals(NUM_OBJECTS, objects.size(), "list is wrong size");

        // loop through all objects.. make sure some have children and some have data and that data is never above
        // threshold
        VerificationResults results = new VerificationResults();
        verify(testSource, objects, results);

        Assertions.assertTrue(results.maxSize < MAX_SIZE, "max size exceeded");
        Assertions.assertTrue(results.hasDirectories, "no directories detected");
        Assertions.assertTrue(results.hasData, "no data detected");

        System.out.println(String.format("total objects: %s, dirs: %s, data objects: %s, total size: %s",
                results.totalObjects, results.totalDirs, results.totalDataObjects, results.totalSize));
    }

    private void verify(TestStorage storage, Collection<? extends SyncObject> objects, VerificationResults results) {
        for (SyncObject object : objects) {
            results.totalObjects++;
            if (object.getMetadata().isDirectory()) {
                results.totalDirs++;
                results.hasDirectories = true;
                verify(storage, storage.getChildren(storage.getIdentifier(object.getRelativePath(), true)), results);
            } else {
                int objectSize = (int) object.getMetadata().getContentLength();
                results.hasData = true;
                results.totalDataObjects++;
                results.totalSize += objectSize;
                if (objectSize > results.maxSize) results.maxSize = objectSize;
            }
        }
    }

    private class VerificationResults {
        boolean hasDirectories = false, hasData = false;
        int maxSize = 0, totalObjects = 0, totalDataObjects = 0, totalDirs = 0, totalSize = 0;
    }
}

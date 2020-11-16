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
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

public class SqlitePerformanceTest {
    @Test
    public void testOverhead() throws Exception {
        TestConfig testConfig = new TestConfig().withObjectCount(1000).withMaxSize(10 * 1024).withObjectOwner("George")
                .withReadData(true).withDiscardData(false);

        // try and maximize efficiency (too many threads might render an invalid test)
        SyncOptions options = new SyncOptions().withThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        options.withVerify(true);

        TestStorage source = new TestStorage();
        source.withConfig(testConfig).withOptions(options);
        source.configure(source, Collections.emptyIterator(), null); // pre-load test objects

        long start = System.nanoTime();

        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setSyncConfig(new SyncConfig().withTarget(testConfig).withOptions(options));
        sync.run();

        long noDbTime = System.nanoTime() - start;
        long totalObjects = sync.getStats().getObjectsComplete();

        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        File dbFile = File.createTempFile("sqlite-perf-test.db", null);
        dbFile.deleteOnExit();
        DbService dbService = new SqliteDbService(dbFile.getPath());
        for (SyncRecord record : dbService.getAllRecords()) {
            // pre-initialize DB
        }

        start = System.nanoTime();

        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withTarget(testConfig).withOptions(options));
        sync.setSource(source);
        sync.setDbService(dbService);
        sync.run();

        long dbTime = System.nanoTime() - start;

        long perObjectOverhead = (dbTime - noDbTime) / totalObjects;

        System.out.println("per object overhead: " + (perObjectOverhead / 1000) + "µs");
        Assert.assertTrue(perObjectOverhead < 5000000); // we need the overhead to be less than 5ms per object
    }
}

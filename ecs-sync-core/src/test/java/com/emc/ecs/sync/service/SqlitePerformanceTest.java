/*
 * Copyright (c) 2015-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;

public class SqlitePerformanceTest {
    // NOTE: this may not be a valid test - it is highly sensitive to the machine's disk performance
    // the intent here is to make sure the database service is efficient and lightweight so as not
    // to bottleneck overall throughput
    @Test
    public void testOverhead() throws Exception {
        TestConfig testConfig = new TestConfig().withObjectCount(1000).withMaxSize(10 * 1024).withObjectOwner("George")
                .withReadData(true).withDiscardData(false);

        // try and maximize efficiency (too many threads might render an invalid test)
        SyncOptions options = new SyncOptions().withThreadCount(Runtime.getRuntime().availableProcessors());
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

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

        File dbFile = File.createTempFile("sqlite-perf-test.db", null);
        dbFile.deleteOnExit();
        DbService dbService = new SqliteDbService(dbFile, false);
        for (SyncRecord ignored : dbService.getAllRecords()) {
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
        Assertions.assertTrue(perObjectOverhead < 15000000); // we need the overhead to be less than 15ms per object
    }
}

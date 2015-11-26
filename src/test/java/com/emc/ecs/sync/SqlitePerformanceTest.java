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
package com.emc.ecs.sync;

import com.emc.ecs.sync.service.DbService;
import com.emc.ecs.sync.service.SqliteDbService;
import com.emc.ecs.sync.test.TestObjectSource;
import com.emc.ecs.sync.test.TestObjectTarget;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class SqlitePerformanceTest {
    @Test
    public void testOverhead() throws Exception {
        TestObjectSource testSource = new TestObjectSource(1000, 10 * 1024, "George");

        // try and maximize efficiency (too many threads might render an invalid test)
        int threads = Runtime.getRuntime().availableProcessors() * 2;

        long start = System.nanoTime();

        EcsSync sync = new EcsSync();
        sync.setSource(testSource);
        sync.setTarget(new TestObjectTarget());
        sync.setSyncThreadCount(threads);
        sync.setVerify(true);
        sync.setLogLevel("quiet");
        sync.run();

        long totalObjects = sync.getObjectsComplete();
        long noDbTime = System.nanoTime() - start;

        Assert.assertEquals(0, sync.getObjectsFailed());

        File dbFile = File.createTempFile("sqlite-perf-test.db", null);
        dbFile.deleteOnExit();
        DbService dbService = new SqliteDbService(dbFile.getPath());

        start = System.nanoTime();

        sync = new EcsSync();
        sync.setDbService(dbService);
        sync.setReprocessObjects(true);
        sync.setSource(testSource);
        sync.setTarget(new TestObjectTarget());
        sync.setSyncThreadCount(threads);
        sync.setVerify(true);
        sync.setLogLevel("quiet");
        sync.run();

        long dbTime = System.nanoTime() - start;

        long perObjectOverhead = (dbTime - noDbTime) / totalObjects;

        System.out.println("per object overhead: " + (perObjectOverhead / 1000) + "µs");
        Assert.assertTrue(perObjectOverhead < 3000000); // we need the overhead to be less than 3ms per object
    }
}

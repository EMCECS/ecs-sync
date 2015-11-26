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

import com.emc.ecs.sync.model.SyncEstimate;
import com.emc.ecs.sync.service.DbService;
import com.emc.ecs.sync.service.SqliteDbService;
import com.emc.ecs.sync.test.TestObjectSource;
import com.emc.ecs.sync.test.TestObjectTarget;
import com.emc.ecs.sync.test.TestSyncObject;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class IterationTest {
    private static final Logger log = LoggerFactory.getLogger(IterationTest.class);

    private static Random random = new Random();

    @Test
    public void testModify() throws Exception {
        TestObjectSource source = new TestObjectSource(100, 10 * 1024, "foo");
        source.configure(null, null, null);
        SyncEstimate estimate = source.createEstimate();

        long initialTime = System.currentTimeMillis();

        modify(source.getObjects(), 25, (int) estimate.getTotalObjectCount());

        Assert.assertEquals(25, countModified(source.getObjects(), initialTime));
    }

    protected int countModified(List<TestSyncObject> objects, long sinceTime) {
        int modified = 0;
        for (TestSyncObject object : objects) {
            if (object.getMetadata().getModificationTime().getTime() > sinceTime) {
                modified++;
                log.info("{} is modified", object.getRelativePath());
            }
            if (object.isDirectory()) modified += countModified(object.getChildren(), sinceTime);
        }
        return modified;
    }

    @Test
    public void testReprocess() throws Exception {
        TestObjectSource source = new TestObjectSource(100, 10 * 1024, "foo");
        source.configure(null, null, null);
        SyncEstimate estimate = source.createEstimate();
        TestObjectTarget target = new TestObjectTarget();
        DbService dbService = new SqliteDbService(":memory:");

        // test sync-only
        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.setReprocessObjects(true);
        sync.run();

        Assert.assertEquals(estimate.getTotalObjectCount(), sync.getObjectsComplete());
        Assert.assertEquals(estimate.getTotalByteCount(), sync.getBytesComplete());
        Assert.assertEquals(0, sync.getObjectsFailed());

        modify(source.getObjects(), 25, (int) estimate.getTotalObjectCount());

        target = new TestObjectTarget();
        sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.setReprocessObjects(true);
        sync.run();

        // all should be reprocessed
        Assert.assertEquals(estimate.getTotalObjectCount(), sync.getObjectsComplete());
        Assert.assertEquals(estimate.getTotalByteCount(), sync.getBytesComplete());
        Assert.assertEquals(0, sync.getObjectsFailed());

        // test verify from sync
        target = new TestObjectTarget();
        sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setVerify(true);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.setReprocessObjects(true);
        sync.run();

        // all should be reprocessed
        Assert.assertEquals(estimate.getTotalObjectCount(), sync.getObjectsComplete());
        Assert.assertEquals(estimate.getTotalByteCount(), sync.getBytesComplete());
        Assert.assertEquals(0, sync.getObjectsFailed());

        // test verify-only after verify
        target = new TestObjectTarget();
        target.ingest(source.getObjects());
        sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setVerifyOnly(true);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.setReprocessObjects(true);
        sync.run();

        // all should be reprocessed
        Assert.assertEquals(estimate.getTotalObjectCount(), sync.getObjectsComplete());
        Assert.assertEquals(estimate.getTotalByteCount(), sync.getBytesComplete());
        Assert.assertEquals(0, sync.getObjectsFailed());
    }

    @Test
    public void testSkipProcessed() throws Exception {
        TestObjectSource source = new TestObjectSource(100, 10 * 1024, "foo");
        source.configure(null, null, null);
        SyncEstimate estimate = source.createEstimate();
        TestObjectTarget target = new TestObjectTarget();
        DbService dbService = new SqliteDbService(":memory:");

        // test sync-only
        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.run();

        Assert.assertEquals(estimate.getTotalObjectCount(), sync.getObjectsComplete());
        Assert.assertEquals(estimate.getTotalByteCount(), sync.getBytesComplete());
        Assert.assertEquals(0, sync.getObjectsFailed());

        target = new TestObjectTarget();
        sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.run();

        // none should be reprocessed
        Assert.assertEquals(0, sync.getObjectsComplete());
        Assert.assertEquals(0, sync.getBytesComplete());
        Assert.assertEquals(0, sync.getObjectsFailed());

        // test verify from sync
        target = new TestObjectTarget();
        target.ingest(source.getObjects());
        sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setVerify(true);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.run();

        // all should be reprocessed
        Assert.assertEquals(estimate.getTotalObjectCount(), sync.getObjectsComplete());
        Assert.assertEquals(estimate.getTotalByteCount(), sync.getBytesComplete());
        Assert.assertEquals(0, sync.getObjectsFailed());

        // test verify-only after verify
        target = new TestObjectTarget();
        target.ingest(source.getObjects());
        sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setVerifyOnly(true);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.run();

        // none should be reprocessed
        Assert.assertEquals(0, sync.getObjectsComplete());
        Assert.assertEquals(0, sync.getBytesComplete());
        Assert.assertEquals(0, sync.getObjectsFailed());
    }

    @Test
    public void testModifiedOnly() throws Exception {
        TestObjectSource source = new TestObjectSource(100, 10 * 1024, "foo");
        source.configure(null, null, null);
        SyncEstimate estimate = source.createEstimate();
        TestObjectTarget target = new TestObjectTarget();
        DbService dbService = new SqliteDbService(":memory:");

        // test sync-only
        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.run();

        Assert.assertEquals(estimate.getTotalObjectCount(), sync.getObjectsComplete());
        Assert.assertEquals(estimate.getTotalByteCount(), sync.getBytesComplete());
        Assert.assertEquals(0, sync.getObjectsFailed());

        modify(source.getObjects(), 25, (int) estimate.getTotalObjectCount());

        target = new TestObjectTarget();
        sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.run();

        // only modified should be reprocessed
        Assert.assertEquals(25, sync.getObjectsComplete());
        Assert.assertTrue(sync.getBytesComplete() > 0);
        Assert.assertEquals(0, sync.getObjectsFailed());

        // test verify from sync
        target = new TestObjectTarget();
        target.ingest(source.getObjects());
        sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setVerify(true);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.run();

        // all should be reprocessed
        Assert.assertEquals(estimate.getTotalObjectCount(), sync.getObjectsComplete());
        Assert.assertEquals(estimate.getTotalByteCount(), sync.getBytesComplete());
        Assert.assertEquals(0, sync.getObjectsFailed());

        // test verify-only after verify
        // this is a strange test since verify-only would likely fail for any objects that have
        // been updated in the source, but they will happily succeed here since no data was changed
        modify(source.getObjects(), 25, (int) estimate.getTotalObjectCount());

        target = new TestObjectTarget();
        target.ingest(source.getObjects());
        sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setVerifyOnly(true);
        sync.setSyncThreadCount(Runtime.getRuntime().availableProcessors() * 2);
        sync.setDbService(dbService);
        sync.run();

        // only modified should be reprocessed
        Assert.assertEquals(0, sync.getObjectsFailed());
        Assert.assertEquals(25, sync.getObjectsComplete());
        Assert.assertTrue(sync.getBytesComplete() > 0);
    }

    protected void modify(List<TestSyncObject> objects, int toModify, int totalCount) throws InterruptedException {
        Thread.sleep(100);
        Set<Integer> modifiedIndexes = new HashSet<>();
        List<String> modified = new ArrayList<>();
        for (int i = 0; i < toModify; i++) {
            int index = random.nextInt(totalCount);
            while (modifiedIndexes.contains(index)) index = random.nextInt(totalCount);
            modifiedIndexes.add(index);
            log.info("modifying index {}", index);
            modifyAtCrawlIndex(objects, new AtomicInteger(index), modified);
        }
        Collections.sort(modified);
        for (String path : modified) {
            log.info("modified {}", path);
        }
    }

    protected void modifyAtCrawlIndex(List<TestSyncObject> objects, AtomicInteger crawlIndex, List<String> modified) {
        for (TestSyncObject object : objects) {
            if (crawlIndex.decrementAndGet() == 0) {
                object.getMetadata().setModificationTime(new Date());
                modified.add(object.getRelativePath());
                return;
            }
            if (object.isDirectory()) modifyAtCrawlIndex(object.getChildren(), crawlIndex, modified);
        }
    }
}

/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.service.DbService;
import com.emc.ecs.sync.service.SqliteDbService;
import com.emc.ecs.sync.storage.TestStorage;
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
        TestConfig testConfig = new TestConfig().withObjectCount(100).withMaxSize(10 * 1024).withObjectOwner("foo")
                .withReadData(false).withDiscardData(false);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withSource(testConfig).withTarget(testConfig));
        sync.run();

        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
        Assert.assertEquals(sync.getEstimatedTotalObjects(), sync.getStats().getObjectsComplete());

        long initialTime = System.currentTimeMillis();

        TestStorage source = (TestStorage) sync.getSource();

        modify(source, source.getRootObjects(), 25, (int) sync.getEstimatedTotalObjects());

        Assert.assertEquals(25, countModified(source, source.getRootObjects(), initialTime));
    }

    private int countModified(TestStorage storage, Collection<? extends SyncObject> objects, long sinceTime) {
        int modified = 0;
        for (SyncObject object : objects) {
            if (object.getMetadata().getModificationTime().getTime() > sinceTime) {
                modified++;
                log.info("{} is modified", object.getRelativePath());
            }
            if (object.getMetadata().isDirectory())
                modified += countModified(storage, storage.getChildren(storage.getIdentifier(object.getRelativePath(), true)), sinceTime);
        }
        return modified;
    }

    @Test
    public void testSkipProcessed() throws Exception {
        TestConfig testConfig = new TestConfig().withObjectCount(100).withMaxSize(10 * 1024).withObjectOwner("foo")
                .withDiscardData(false).withReadData(true);

        DbService dbService = new SqliteDbService(":memory:");

        SyncOptions options = new SyncOptions().withThreadCount(Runtime.getRuntime().availableProcessors() * 2);

        // test sync-only
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withSource(testConfig).withTarget(testConfig).withOptions(options));
        sync.setDbService(dbService);
        sync.run();

        Assert.assertEquals(sync.getEstimatedTotalObjects(), sync.getStats().getObjectsComplete());
        Assert.assertEquals(sync.getEstimatedTotalBytes(), sync.getStats().getBytesComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        long totalObjects = sync.getEstimatedTotalObjects(), totalBytes = sync.getEstimatedTotalBytes();

        TestStorage source = (TestStorage) sync.getSource();

        TestStorage target = new TestStorage();
        target.setConfig(new TestConfig().withReadData(true).withDiscardData(false));
        target.ingest(source, null);

        // test re-run
        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options));
        sync.setSource(source);
        sync.setTarget(target);
        sync.setDbService(dbService);
        sync.run();

        // none should be reprocessed
        Assert.assertEquals(0, sync.getStats().getObjectsComplete());
        Assert.assertEquals(0, sync.getStats().getBytesComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        // test verify from sync
        options.setVerify(true);

        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options));
        sync.setSource(source);
        sync.setTarget(target);
        sync.setDbService(dbService);
        sync.run();

        options.setVerify(false); // revert run-specific options

        // all should be reprocessed
        Assert.assertEquals(totalObjects, sync.getStats().getObjectsComplete());
        Assert.assertEquals(totalBytes, sync.getStats().getBytesComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        // test verify-only after verify
        options.setVerifyOnly(true);

        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options));
        sync.setSource(source);
        sync.setTarget(target);
        sync.setDbService(dbService);
        sync.run();

        options.setVerifyOnly(false); // revert run-specific options

        // none should be reprocessed
        Assert.assertEquals(0, sync.getStats().getObjectsComplete());
        Assert.assertEquals(0, sync.getStats().getBytesComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
    }

    @Test
    public void testModifiedOnly() throws Exception {
        TestConfig testConfig = new TestConfig().withObjectCount(100).withMaxSize(10 * 1024).withObjectOwner("foo")
                .withDiscardData(false).withReadData(true);

        DbService dbService = new SqliteDbService(":memory:");

        SyncOptions options = new SyncOptions().withThreadCount(Runtime.getRuntime().availableProcessors() * 2);

        // test sync-only
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withSource(testConfig).withTarget(testConfig).withOptions(options));
        sync.setDbService(dbService);
        sync.run();

        Assert.assertEquals(sync.getEstimatedTotalObjects(), sync.getStats().getObjectsComplete());
        Assert.assertEquals(sync.getEstimatedTotalBytes(), sync.getStats().getBytesComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        long totalObjects = sync.getEstimatedTotalObjects(), totalBytes = sync.getEstimatedTotalBytes();

        TestStorage source = (TestStorage) sync.getSource();

        // make sure mtime is aged
        Thread.sleep(1000);

        modify(source, source.getRootObjects(), 25, (int) totalObjects);

        // 2nd sync with modifications
        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withTarget(testConfig).withOptions(options));
        sync.setSource(source);
        sync.setDbService(dbService);
        sync.run();

        // only modified should be reprocessed
        Assert.assertEquals(25, sync.getStats().getObjectsComplete());
        Assert.assertTrue(sync.getStats().getBytesComplete() > 0);
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        TestStorage target = new TestStorage();
        target.setConfig(new TestConfig().withReadData(true).withDiscardData(false));
        target.ingest(source, null);

        // test verify from sync
        options.setVerify(true);

        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options));
        sync.setSource(source);
        sync.setTarget(target);
        sync.setDbService(dbService);
        sync.run();

        options.setVerify(false); // revert run-specific options

        // all should be reprocessed
        Assert.assertEquals(totalObjects, sync.getStats().getObjectsComplete());
        Assert.assertEquals(totalBytes, sync.getStats().getBytesComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        // make sure mtime is aged
        Thread.sleep(500);

        // test verify-only after verify
        // this is a strange test since verify-only would likely fail for any objects that have
        // been updated in the source, but they will happily succeed here since no data was changed
        modify(source, source.getRootObjects(), 25, (int) totalObjects);

        options.setVerifyOnly(true);

        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options));
        sync.setSource(source);
        sync.setTarget(target);
        sync.setDbService(dbService);
        sync.run();

        options.setVerifyOnly(false); // revert run-specific options

        // only modified should be reprocessed
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
        Assert.assertEquals(25, sync.getStats().getObjectsComplete());
        Assert.assertTrue(sync.getStats().getBytesComplete() > 0);
    }

    private void modify(TestStorage storage, List<? extends SyncObject> objects, int toModify, int totalCount) throws InterruptedException {
        Thread.sleep(100);
        Set<Integer> modifiedIndexes = new HashSet<>();
        List<String> modified = new ArrayList<>();
        for (int i = 0; i < toModify; i++) {
            int index = random.nextInt(totalCount);
            while (modifiedIndexes.contains(index)) index = random.nextInt(totalCount);
            modifiedIndexes.add(index);
            log.info("modifying index {}", index);
            modifyAtCrawlIndex(storage, objects, new AtomicInteger(index), modified);
        }
        Collections.sort(modified);
        for (String path : modified) {
            log.info("modified {}", path);
        }
    }

    private void modifyAtCrawlIndex(TestStorage storage, Collection<? extends SyncObject> objects, AtomicInteger crawlIndex, List<String> modified) {
        for (SyncObject object : objects) {
            if (crawlIndex.decrementAndGet() == 0) {
                object.getMetadata().setModificationTime(new Date());
                modified.add(object.getRelativePath());
                return;
            }
            if (object.getMetadata().isDirectory())
                modifyAtCrawlIndex(storage, storage.getChildren(storage.getIdentifier(object.getRelativePath(), true)), crawlIndex, modified);
        }
    }
}

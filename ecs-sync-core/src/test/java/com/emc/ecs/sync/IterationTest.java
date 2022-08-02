/*
 * Copyright (c) 2015-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync;

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.service.DbService;
import com.emc.ecs.sync.service.InMemoryDbService;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class IterationTest {
    private static final Logger log = LoggerFactory.getLogger(IterationTest.class);

    private static final Random random = new Random();

    @Test
    public void testModify() throws Exception {
        TestConfig testConfig = new TestConfig().withObjectCount(100).withMaxSize(10 * 1024).withObjectOwner("foo")
                .withReadData(false).withDiscardData(false);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withSource(testConfig).withTarget(testConfig));
        sync.run();

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(sync.getEstimatedTotalObjects(), sync.getStats().getObjectsComplete());

        long initialTime = System.currentTimeMillis();

        TestStorage source = (TestStorage) sync.getSource();

        modify(source, source.getRootObjects(), 25, (int) sync.getEstimatedTotalObjects());

        Assertions.assertEquals(25, countModified(source, source.getRootObjects(), initialTime));
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
    public void testSkipProcessedWithDbService() {
        testSkipProcessed(true);
    }

    @Test
    public void testSkipProcessedNoDbService() {
        testSkipProcessed(false);
    }

    private void testSkipProcessed(boolean setDbService) {
        TestConfig testConfig = new TestConfig().withObjectCount(100).withMaxSize(10 * 1024).withObjectOwner("foo")
                .withDiscardData(false).withReadData(true);

        SyncOptions options = new SyncOptions().withThreadCount(Runtime.getRuntime().availableProcessors() * 2);

        // test sync-only
        EcsSync sync = new EcsSync();
        DbService dbService = null;
        if (setDbService) {
            dbService = new InMemoryDbService(false);
            sync.setDbService(dbService);
        } else {
            // It is possible that a child is created before its parent directory (due to its task/thread executing faster), and
            // have no way of detecting that. Here we don't generate directory to test Skip/Processed for NoDbService because
            // directory metadata must be *always* updated and directory won't be skipped.
            testConfig.withChanceOfChildren(0);
        }
        sync.setSyncConfig(new SyncConfig().withSource(testConfig).withTarget(testConfig).withOptions(options));
        sync.run();

        Assertions.assertEquals(sync.getEstimatedTotalObjects(), sync.getStats().getObjectsComplete());
        Assertions.assertEquals(sync.getEstimatedTotalBytes(), sync.getStats().getBytesComplete());
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(0, sync.getStats().getObjectsSkipped());
        Assertions.assertEquals(0, sync.getStats().getBytesSkipped());
        Assertions.assertEquals(0, sync.getStats().getObjectsCopySkipped());
        Assertions.assertEquals(0, sync.getStats().getBytesCopySkipped());

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
        if (setDbService) sync.setDbService(dbService);
        sync.run();

        // none should be reprocessed
        Assertions.assertEquals(0, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(0, sync.getStats().getBytesComplete());
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(totalObjects, sync.getStats().getObjectsSkipped());
        Assertions.assertEquals(totalBytes, sync.getStats().getBytesSkipped());
        Assertions.assertEquals(totalObjects, sync.getStats().getObjectsCopySkipped());
        Assertions.assertEquals(totalBytes, sync.getStats().getBytesCopySkipped());

        // test verify from sync
        options.setVerify(true);

        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options));
        sync.setSource(source);
        sync.setTarget(target);
        if (setDbService) sync.setDbService(dbService);
        sync.run();

        options.setVerify(false); // revert run-specific options

        // all should be reprocessed for verification
        Assertions.assertEquals(totalObjects, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(totalBytes, sync.getStats().getBytesComplete());
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(0, sync.getStats().getObjectsSkipped());
        Assertions.assertEquals(0, sync.getStats().getBytesSkipped());
        Assertions.assertEquals(totalObjects, sync.getStats().getObjectsCopySkipped());
        Assertions.assertEquals(totalBytes, sync.getStats().getBytesCopySkipped());

        // test verify-only after verify
        options.setVerifyOnly(true);

        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options));
        sync.setSource(source);
        sync.setTarget(target);
        if (setDbService) sync.setDbService(dbService);
        sync.run();

        options.setVerifyOnly(false); // revert run-specific options

        if (setDbService) {
            // none should be reprocessed because verification status is kept in DB.
            Assertions.assertEquals(0, sync.getStats().getObjectsComplete());
            Assertions.assertEquals(0, sync.getStats().getBytesComplete());
            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(totalObjects, sync.getStats().getObjectsSkipped());
            Assertions.assertEquals(totalBytes, sync.getStats().getBytesSkipped());
            Assertions.assertEquals(0, sync.getStats().getObjectsCopySkipped());
            Assertions.assertEquals(0, sync.getStats().getBytesCopySkipped());
        }
        else {
            // all should be reprocessed because verification status is not kept.
            Assertions.assertEquals(totalObjects, sync.getStats().getObjectsComplete());
            Assertions.assertEquals(totalBytes, sync.getStats().getBytesComplete());
            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(0, sync.getStats().getObjectsSkipped());
            Assertions.assertEquals(0, sync.getStats().getBytesSkipped());
            Assertions.assertEquals(0, sync.getStats().getObjectsCopySkipped());
            Assertions.assertEquals(0, sync.getStats().getBytesCopySkipped());
        }
    }

    @Test
    public void testModifiedOnly() throws Exception {
        TestConfig testConfig = new TestConfig().withObjectCount(100).withMaxSize(10 * 1024).withObjectOwner("foo")
                .withDiscardData(false).withReadData(true);

        DbService dbService = new InMemoryDbService(false);

        SyncOptions options = new SyncOptions().withThreadCount(Runtime.getRuntime().availableProcessors() * 2);

        // test sync-only
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withSource(testConfig).withTarget(testConfig).withOptions(options));
        sync.setDbService(dbService);
        sync.run();

        Assertions.assertEquals(sync.getEstimatedTotalObjects(), sync.getStats().getObjectsComplete());
        Assertions.assertEquals(sync.getEstimatedTotalBytes(), sync.getStats().getBytesComplete());
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

        long totalObjects = sync.getEstimatedTotalObjects(), totalBytes = sync.getEstimatedTotalBytes();

        TestStorage source = (TestStorage) sync.getSource();

        modify(source, source.getRootObjects(), 25, (int) totalObjects);

        // 2nd sync with modifications
        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withTarget(testConfig).withOptions(options));
        sync.setSource(source);
        sync.setDbService(dbService);
        sync.run();

        // only modified should be reprocessed
        Assertions.assertEquals(25, sync.getStats().getObjectsComplete());
        Assertions.assertTrue(sync.getStats().getBytesComplete() > 0);
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

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
        Assertions.assertEquals(totalObjects, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(totalBytes, sync.getStats().getBytesComplete());
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

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
        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(25, sync.getStats().getObjectsComplete());
        Assertions.assertTrue(sync.getStats().getBytesComplete() > 0);
    }

    private void modify(TestStorage storage, List<? extends SyncObject> objects, int toModify, int totalCount) throws InterruptedException {
        Thread.sleep(2000);
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
            if (crawlIndex.getAndDecrement() == 0) {
                object.getMetadata().setModificationTime(new Date());
                modified.add(object.getRelativePath());
                return;
            }
            if (object.getMetadata().isDirectory())
                modifyAtCrawlIndex(storage, storage.getChildren(storage.getIdentifier(object.getRelativePath(), true)), crawlIndex, modified);
        }
    }
}

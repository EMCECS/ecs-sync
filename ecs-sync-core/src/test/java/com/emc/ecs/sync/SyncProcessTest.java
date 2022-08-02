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
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.filter.AbstractFilter;
import com.emc.ecs.sync.filter.InternalFilter;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.service.DbService;
import com.emc.ecs.sync.service.InMemoryDbService;
import com.emc.ecs.sync.service.SyncRecord;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.test.DelayFilter;
import com.emc.ecs.sync.test.TestUtil;
import com.emc.ecs.sync.util.OptionChangeListener;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncProcessTest {
    @Test
    public void testSourceObjectNotFound() throws Exception {
        com.emc.ecs.sync.config.storage.TestConfig testConfig = new com.emc.ecs.sync.config.storage.TestConfig();
        testConfig.withObjectCount(0).withReadData(true).withDiscardData(false);

        File sourceIdList = TestUtil.writeTempFile("this-id-does-not-exist");

        SyncOptions options = new SyncOptions().withRememberFailed(true).withSourceListFile(sourceIdList.getAbsolutePath());

        SyncConfig syncConfig = new SyncConfig().withOptions(options).withSource(testConfig).withTarget(testConfig);

        DbService dbService = new InMemoryDbService(false);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setDbService(dbService);
        sync.run();

        Assertions.assertEquals(0, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(1, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(1, sync.getStats().getFailedObjects().size());
        String failedId = sync.getStats().getFailedObjects().iterator().next();
        Assertions.assertNotNull(failedId);

        ObjectContext context = new ObjectContext().withOptions(options).withSourceSummary(new ObjectSummary(failedId, false, 0));
        SyncRecord syncRecord = dbService.getSyncRecord(context);
        Assertions.assertNotNull(syncRecord);
        Assertions.assertEquals(ObjectStatus.Error, syncRecord.getStatus());
    }

    @Test
    public void testRetryQueue() throws Exception {
        int retries = 3;

        com.emc.ecs.sync.config.storage.TestConfig testConfig = new com.emc.ecs.sync.config.storage.TestConfig();
        testConfig.withObjectCount(100).withMaxSize(1024).withObjectOwner("Boo Radley").withReadData(true).withDiscardData(false);

        DelayFilter.DelayConfig delayConfig = new DelayFilter.DelayConfig().withDelayMs(100);
        ErrorThrowingConfig filterConfig = new ErrorThrowingConfig().withRetriesExpected(retries);

        SyncOptions options = new SyncOptions().withThreadCount(8).withRetryAttempts(retries);

        SyncConfig syncConfig = new SyncConfig().withOptions(options).withSource(testConfig).withTarget(testConfig);
        syncConfig.withFilters(Arrays.asList(delayConfig, filterConfig));

        final EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        ExecutorService service = Executors.newSingleThreadExecutor();
        Future<?> future = service.submit(sync);
        service.shutdown();

        int time = 0;
        while (!sync.isRunning() && time++ < 20) Thread.sleep(500); // wait for threads to kick off
        Thread.sleep(500); // wait for retries to queue
        Assertions.assertTrue(sync.getObjectsAwaitingRetry() > 0);

        future.get(); // wait for sync to finish

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(sync.getStats().getObjectsComplete(), sync.getEstimatedTotalObjects());
        Assertions.assertEquals((retries + 1) * sync.getStats().getObjectsComplete(), filterConfig.getTotalAttempts().get());
    }

    @Test
    public void testOptionsChangedListener() throws Exception {
        com.emc.ecs.sync.config.storage.TestConfig testConfig = new com.emc.ecs.sync.config.storage.TestConfig();
        testConfig.withObjectCount(500).withMaxSize(1024).withObjectOwner("Boo Radley").withReadData(true).withDiscardData(false);

        SyncOptions options = new SyncOptions().withThreadCount(2);

        ChangeListenerConfig filterConfig = new ChangeListenerConfig();

        SyncConfig syncConfig = new SyncConfig().withOptions(options).withSource(testConfig).withTarget(testConfig);
        syncConfig.withFilters(Collections.singletonList(filterConfig));

        final EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        ExecutorService service = Executors.newSingleThreadExecutor();
        Future future = service.submit(new Runnable() {
            @Override
            public void run() {
                sync.run();
            }
        });
        service.shutdown();

        int time = 0;
        while (!sync.isRunning() && time++ < 20) Thread.sleep(200); // wait for threads to kick off

        // change thread count; this should trigger the change event
        sync.setThreadCount(32);

        future.get(); // wait for sync to finish

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(sync.getStats().getObjectsComplete(), sync.getEstimatedTotalObjects());
        Assertions.assertTrue(filterConfig.isChangeFired());
    }

    @Test
    public void testQueryError() throws Exception {
        int fileCount = 5;
        byte[] bytes = "".getBytes(StandardCharsets.UTF_8);

        // create temp dir
        Path tempPath = Files.createTempDirectory("query-error-test");
        // create sub-dir
        Path subPath = Files.createDirectory(tempPath.resolve("foo"));
        // write files to parent
        for (int i = 0; i < fileCount; i++) {
            Files.write(tempPath.resolve("file-" + i), bytes);
        }
        // write files to child
        for (int i = 0; i < fileCount; i++) {
            Files.write(subPath.resolve("file-" + i), bytes);
        }

        try {
            // remove read permission from parent
            Files.setPosixFilePermissions(subPath, PosixFilePermissions.fromString("-wx------"));

            // sync
            FilesystemConfig sourceConfig = new FilesystemConfig();
            sourceConfig.setPath(tempPath.toString());
            TestConfig testConfig = new TestConfig();
            SyncOptions options = new SyncOptions().withThreadCount(2).withRememberFailed(true);
            SyncConfig syncConfig = new SyncConfig().withOptions(options).withSource(sourceConfig).withTarget(testConfig);

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.run();

            // query failures only show up in stats and in the log (there's no way to track them in the DB yet)
            Assertions.assertEquals(1, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(6, sync.getStats().getObjectsComplete());
            Assertions.assertTrue(sync.getStats().getFailedObjects().iterator().next().endsWith("/foo"));
        } finally {
            Files.setPosixFilePermissions(subPath, PosixFilePermissions.fromString("rwx------"));
            for (File file : subPath.toFile().listFiles()) {
                Files.delete(file.toPath());
            }
            Files.delete(subPath);
            for (File file : tempPath.toFile().listFiles()) {
                Files.delete(file.toPath());
            }
            Files.delete(tempPath);
        }
    }

    @Test
    public void testDuplicatesInSourceList() throws Exception {
        SyncOptions options = new SyncOptions();

        // construct source storage
        TestConfig testConfig = new TestConfig().withDiscardData(false);
        TestStorage source = new TestStorage();
        source.withConfig(testConfig).withOptions(options);

        // ingest one object
        String identifier = source.getIdentifier("foo", false);
        source.updateObject(identifier,
                new SyncObject(source, "foo", new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl()));

        // create source list file with x duplicates
        int x = 12;
        StringBuilder list = new StringBuilder();
        for (int i = 0; i < x; i++) {
            list.append(identifier).append("\n");
        }
        Path sourceListPath = Files.createTempFile("dup-source-list", null);
        Files.write(sourceListPath, list.toString().getBytes(StandardCharsets.UTF_8));

        options.setSourceListFile(sourceListPath.toString());

        // create a DB table (this is the root of the issue)
        DbService dbService = new InMemoryDbService(false);

        SyncConfig syncConfig = new SyncConfig().withOptions(options).withTarget(new TestConfig());

        // run sync
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(source);
        sync.setDbService(dbService);
        sync.run();

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
        Assertions.assertEquals(x - 1, sync.getStats().getObjectsSkipped());

        // count DB records
        int count = 0;
        for (SyncRecord record : dbService.getAllRecords()) {
            count++;
        }
        Assertions.assertEquals(1, count);
        Files.delete(sourceListPath);
    }

    @Test
    public void testFailedObjects() throws Exception {
        int totalObjects = 20;
        String[] sourceList = new String[totalObjects];
        for (int i = 0; i < totalObjects; i++) {
            sourceList[i] = "prefix" + i + "/object" + i;
        }

        TestConfig testConfig = new TestConfig();
        testConfig.withObjectCount(0).withReadData(true).withDiscardData(false);

        SyncOptions options = new SyncOptions().withRememberFailed(true).withSourceList(sourceList);
        SyncConfig syncConfig = new SyncConfig().withOptions(options).withSource(testConfig).withTarget(testConfig);

        final EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.run();

        Assertions.assertEquals(totalObjects, sync.getStats().getObjectsFailed());
        int idx = 0;
        for (FailedObject failedObject : sync.getStats().getFailedObjectDetails()) {
            Assertions.assertTrue(failedObject.getIdentifier().equals(sourceList[idx++]));
        }
    }

    @FilterConfig(cliName = "98s76df8s7d6fs87d6f")
    @InternalFilter
    public static class ErrorThrowingConfig {
        private int retriesExpected;
        private AtomicInteger totalAttempts = new AtomicInteger();

        public int getRetriesExpected() {
            return retriesExpected;
        }

        public void setRetriesExpected(int retriesExpected) {
            this.retriesExpected = retriesExpected;
        }

        public ErrorThrowingConfig withRetriesExpected(int retriesExpected) {
            setRetriesExpected(retriesExpected);
            return this;
        }

        public AtomicInteger getTotalAttempts() {
            return totalAttempts;
        }

        public void setTotalAttempts(AtomicInteger totalAttempts) {
            this.totalAttempts = totalAttempts;
        }

        public ErrorThrowingConfig withTotalTransfers(AtomicInteger totalTransfers) {
            setTotalAttempts(totalTransfers);
            return this;
        }
    }

    public static class ErrorThrowingFilter extends AbstractFilter<ErrorThrowingConfig> {
        @Override
        public void filter(ObjectContext objectContext) {
            config.getTotalAttempts().incrementAndGet();
            if (objectContext.getFailures() == config.getRetriesExpected()) getNext().filter(objectContext);
            else throw new RuntimeException("Nope, not yet (" + objectContext.getFailures() + ")");
        }

        @Override
        public SyncObject reverseFilter(ObjectContext objectContext) {
            return getNext().reverseFilter(objectContext);
        }
    }

    @FilterConfig(cliName = "7s6df7s6dfd7")
    @InternalFilter
    public static class ChangeListenerConfig {
        private boolean changeFired;

        public boolean isChangeFired() {
            return changeFired;
        }

        public void setChangeFired(boolean changeFired) {
            this.changeFired = changeFired;
        }
    }

    public static class ChangeListenerFilter extends AbstractFilter<ChangeListenerConfig> implements OptionChangeListener {
        @Override
        public void optionsChanged(SyncOptions options) {
            config.setChangeFired(true);
        }

        @Override
        public void filter(ObjectContext objectContext) {
            getNext().filter(objectContext);
        }

        @Override
        public SyncObject reverseFilter(ObjectContext objectContext) {
            return getNext().reverseFilter(objectContext);
        }
    }
}

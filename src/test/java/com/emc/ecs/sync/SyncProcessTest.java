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

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.filter.AbstractFilter;
import com.emc.ecs.sync.filter.InternalFilter;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectStatus;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.service.DbService;
import com.emc.ecs.sync.service.SqliteDbService;
import com.emc.ecs.sync.service.SyncRecord;
import com.emc.ecs.sync.test.TestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
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

        DbService dbService = new SqliteDbService(":memory:");

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setDbService(dbService);
        sync.run();

        Assert.assertEquals(0, sync.getStats().getObjectsComplete());
        Assert.assertEquals(1, sync.getStats().getObjectsFailed());
        Assert.assertEquals(1, sync.getStats().getFailedObjects().size());
        String failedId = sync.getStats().getFailedObjects().iterator().next();
        Assert.assertNotNull(failedId);

        ObjectContext context = new ObjectContext().withOptions(options).withSourceSummary(new ObjectSummary(failedId, false, 0));
        SyncRecord syncRecord = dbService.getSyncRecord(context);
        Assert.assertNotNull(syncRecord);
        Assert.assertEquals(ObjectStatus.Error, syncRecord.getStatus());
    }

    @Test
    public void testRetryQueue() throws Exception {
        int retries = 3;

        com.emc.ecs.sync.config.storage.TestConfig testConfig = new com.emc.ecs.sync.config.storage.TestConfig();
        testConfig.withObjectCount(500).withMaxSize(1024).withObjectOwner("Boo Radley").withReadData(true).withDiscardData(false);

        ErrorThrowingConfig filterConfig = new ErrorThrowingConfig().withRetriesExpected(retries);

        SyncOptions options = new SyncOptions().withThreadCount(16).withRetryAttempts(retries);

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

        while (!sync.isRunning()) Thread.sleep(200); // wait for threads to kick off
        Assert.assertTrue(sync.getObjectsAwaitingRetry() > 0);

        future.get(); // wait for sync to finish

        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
        Assert.assertEquals(sync.getStats().getObjectsComplete(), sync.getEstimatedTotalObjects());
        Assert.assertEquals((retries + 1) * sync.getStats().getObjectsComplete(), filterConfig.getTotalAttempts().get());
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
}

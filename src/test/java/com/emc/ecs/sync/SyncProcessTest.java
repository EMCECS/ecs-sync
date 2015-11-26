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

import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.ObjectStatus;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.service.DbService;
import com.emc.ecs.sync.service.SqliteDbService;
import com.emc.ecs.sync.service.SyncRecord;
import com.emc.ecs.sync.source.EcsS3Source;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.test.*;
import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.rest.smart.ecs.Vdc;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncProcessTest {
    @Test
    public void testSourceObjectNotFound() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
        String bucket = "ecs-sync-test-source-not-found";
        String endpoint = syncProperties.getProperty(SyncConfig.PROP_S3_ENDPOINT);
        String accessKey = syncProperties.getProperty(SyncConfig.PROP_S3_ACCESS_KEY_ID);
        String secretKey = syncProperties.getProperty(SyncConfig.PROP_S3_SECRET_KEY);
        boolean useVHost = Boolean.valueOf(syncProperties.getProperty(SyncConfig.PROP_S3_VHOST));
        Assume.assumeNotNull(endpoint, accessKey, secretKey);
        URI endpointUri = new URI(endpoint);

        S3Config s3Config;
        if (useVHost) s3Config = new S3Config(endpointUri);
        else s3Config = new S3Config(Protocol.valueOf(endpointUri.getScheme().toUpperCase()), endpointUri.getHost());
        s3Config.withPort(endpointUri.getPort()).withUseVHost(useVHost).withIdentity(accessKey).withSecretKey(secretKey);

        S3Client s3 = new S3JerseyClient(s3Config);

        try {
            s3.createBucket(bucket);
        } catch (S3Exception e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        try {
            File sourceKeyList = TestUtil.writeTempFile("this-key-does-not-exist");

            EcsS3Source source = new EcsS3Source();
            source.setEndpoint(endpointUri);
            source.setProtocol(endpointUri.getScheme());
            source.setVdcs(Collections.singletonList(new Vdc(endpointUri.getHost())));
            source.setPort(endpointUri.getPort());
            source.setEnableVHosts(useVHost);
            source.setAccessKey(accessKey);
            source.setSecretKey(secretKey);
            source.setBucketName(bucket);
            source.setSourceKeyList(sourceKeyList);

            DbService dbService = new SqliteDbService(":memory:");

            EcsSync sync = new EcsSync();
            sync.setSource(source);
            sync.setTarget(new TestObjectTarget());
            sync.setDbService(dbService);

            sync.run();

            Assert.assertEquals(0, sync.getObjectsComplete());
            Assert.assertEquals(1, sync.getObjectsFailed());
            Assert.assertEquals(1, sync.getFailedObjects().size());
            SyncObject syncObject = sync.getFailedObjects().iterator().next();
            Assert.assertNotNull(syncObject);
            SyncRecord syncRecord = dbService.getSyncRecord(sync.getFailedObjects().iterator().next());
            Assert.assertNotNull(syncRecord);
            Assert.assertEquals(ObjectStatus.Error, syncRecord.getStatus());
        } finally {
            try {
                s3.deleteBucket(bucket);
            } catch (Throwable t) {
                // ignore
            }
        }
    }

    @Test
    public void testRetryQueue() {
        int retries = 3;

        TestObjectSource source = new TestObjectSource(500, 1024, "Boo Radley");
        TestObjectTarget target = new TestObjectTarget();
        ErrorThrowingFilter filter = new ErrorThrowingFilter(retries);

        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setQueryThreadCount(32);
        sync.setSyncThreadCount(32);
        sync.setRetryAttempts(retries);
        sync.setFilters(Collections.singletonList((SyncFilter) filter));

        sync.run();

        Assert.assertEquals(0, sync.getObjectsFailed());
        Assert.assertEquals(sync.getObjectsComplete(), sync.getEstimatedTotalObjects());
        Assert.assertEquals((retries + 1) * sync.getObjectsComplete(), filter.getTotalTransfers());
    }

    protected void checkRetries(List<TestSyncObject> objects, int retriesExpected) {
        for (TestSyncObject object : objects) {
            Assert.assertEquals(retriesExpected, object.getFailureCount());
            if (object.isDirectory()) checkRetries(object.getChildren(), retriesExpected);
        }
    }

    protected class ErrorThrowingFilter extends SyncFilter {
        private int retriesExpected;
        private AtomicInteger totalTransfers = new AtomicInteger();

        public ErrorThrowingFilter(int retriesExpected) {
            this.retriesExpected = retriesExpected;
        }

        @Override
        public String getActivationName() {
            return null;
        }

        @Override
        public void filter(SyncObject obj) {
            totalTransfers.incrementAndGet();
            if (obj.getFailureCount() == retriesExpected) getNext().filter(obj);
            else throw new RuntimeException("Nope, not yet (" + obj.getFailureCount() + ")");
        }

        @Override
        public SyncObject reverseFilter(SyncObject obj) {
            return getNext().reverseFilter(obj);
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public String getDocumentation() {
            return null;
        }

        @Override
        public Options getCustomOptions() {
            return null;
        }

        @Override
        protected void parseCustomOptions(CommandLine line) {
        }

        @Override
        public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        }

        public int getTotalTransfers() {
            return totalTransfers.get();
        }
    }
}

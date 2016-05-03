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
package com.emc.ecs.sync.target;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.service.DbService;
import com.emc.ecs.sync.service.SqliteDbService;
import com.emc.ecs.sync.service.SyncRecord;
import com.emc.ecs.sync.test.*;
import com.filepool.fplibrary.*;
import org.junit.*;

import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class CasSimpleTargetTest {

    private static final int OBJ_COUNT = 50;
    private static final int OBJ_MAX_SIZE = 50 * 1024; // 50k

    private static final int SYNC_THREAD_COUNT = 32;
    private static final int SYNC_RETRY_ATTEMPTS = 0;

    private String connectString;
    private String retentionClass;
    private String clipName;
    private Integer retentionPeriod;
    private Integer retentionPeriodEbr;

    private DbService dbService;

    @Before
    public void setup() throws Exception {
        try {
            Properties syncProperties = SyncConfig.getProperties();

            connectString = syncProperties.getProperty(SyncConfig.PROP_CAS_CONNECT_STRING);
            retentionClass = syncProperties.getProperty(SyncConfig.PROP_CAS_RETENTION_CLASS);
            clipName = syncProperties.getProperty(SyncConfig.PROP_CAS_CLIP_NAME);
            if (syncProperties.getProperty(SyncConfig.PROP_CAS_RETENTION_PERIOD) != null)
                retentionPeriod = Integer.parseInt(syncProperties.getProperty(SyncConfig.PROP_CAS_RETENTION_PERIOD));
            if (syncProperties.getProperty(SyncConfig.PROP_CAS_RETENTION_PERIOD_EBR) != null)
                retentionPeriodEbr = Integer.parseInt(syncProperties.getProperty(SyncConfig.PROP_CAS_RETENTION_PERIOD_EBR));

            Assume.assumeNotNull(connectString);
        } catch (FileNotFoundException e) {
            Assume.assumeFalse("Could not load ecs-sync.properties", true);
        }

        dbService = new SqliteDbService(":memory:");
    }

    @After
    public void teardown() throws Exception {
        if (dbService != null) {
            dbService.deleteDatabase();
        }
    }

    @Test
    public void testSyncWithVerification() throws Exception {
        TestObjectSource source = new TestObjectSource(OBJ_COUNT, OBJ_MAX_SIZE, "foo");
        source.configure(null, null, null);
        CasSimpleTarget target = new CasSimpleTarget();
        target.setConnectionString(connectString);
        target.setClipName(clipName);

        IdCollector idCollector = new IdCollector();

        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setFilters(Collections.singletonList((SyncFilter) idCollector));
        sync.setDbService(dbService);
        sync.setSyncThreadCount(SYNC_THREAD_COUNT);
        sync.setRetryAttempts(SYNC_RETRY_ATTEMPTS);
        sync.setVerify(true);

        sync.run();
        try {
            Assert.assertEquals(0, sync.getFailedObjects().size());
            verify(source, target);
        } finally {
            deleteClips(target, idCollector.getIdMap().values());
        }
    }

    @Test
    public void testSyncRetentionClass() throws Exception{
        Assume.assumeNotNull(retentionClass);
        TestObjectSource source = new TestObjectSource(OBJ_COUNT, OBJ_MAX_SIZE, "foo");
        source.configure(null, null, null);
        CasSimpleTarget target = new CasSimpleTarget();
        target.setConnectionString(connectString);
        target.setRetentionClass(retentionClass);

        IdCollector idCollector = new IdCollector();

        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setFilters(Collections.singletonList((SyncFilter) idCollector));
        sync.setDbService(dbService);
        sync.setSyncThreadCount(SYNC_THREAD_COUNT);
        sync.setRetryAttempts(SYNC_RETRY_ATTEMPTS);

        sync.run();
        try {
            Assert.assertEquals(0, sync.getFailedObjects().size());
            verify(source, target);
        } finally {
            deleteClips(target, idCollector.getIdMap().values());
        }
    }

    @Test
    public void testSyncRetentionPeriod() throws Exception{
        Assume.assumeNotNull(retentionPeriod);
        Assume.assumeNotNull(retentionPeriodEbr);
        TestObjectSource source = new TestObjectSource(OBJ_COUNT, OBJ_MAX_SIZE, "foo");
        source.configure(null, null, null);
        CasSimpleTarget target = new CasSimpleTarget();
        target.setConnectionString(connectString);
        target.setRetentionPeriod(retentionPeriod);
        target.setRetentionPeriodEbr(retentionPeriodEbr);

        IdCollector idCollector = new IdCollector();

        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setFilters(Collections.singletonList((SyncFilter) idCollector));
        sync.setDbService(dbService);
        sync.setSyncThreadCount(SYNC_THREAD_COUNT);
        sync.setRetryAttempts(SYNC_RETRY_ATTEMPTS);

        sync.run();
        try {
            Assert.assertEquals(0, sync.getFailedObjects().size());
            verify(source, target);
        } finally {
            deleteClips(target, idCollector.getIdMap().values());
        }
    }

    @Test
    public void testFailures() throws Exception {
        TestObjectSource source = new TestObjectSource(OBJ_COUNT, OBJ_MAX_SIZE, "foo");
        ByteAlteringFilter testFilter = new ByteAlteringFilter();
        source.configure(null, null, null);
        CasSimpleTarget target = new CasSimpleTarget();
        target.setConnectionString(connectString);

        IdCollector idCollector = new IdCollector();

        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setFilters(Collections.singletonList((SyncFilter) testFilter));
        sync.setTarget(target);
        sync.setFilters(Collections.singletonList((SyncFilter) idCollector));
        sync.setDbService(dbService);
        sync.setSyncThreadCount(SYNC_THREAD_COUNT);
        sync.setRetryAttempts(SYNC_RETRY_ATTEMPTS);
        sync.setVerify(true);
        sync.setRetryAttempts(0);

        sync.run();
        try {
            Assert.assertEquals(testFilter.getModifiedObjects(), sync.getObjectsFailed());
        } finally {
            deleteClips(target, idCollector.getIdMap().values());
        }

    }

    @Test
    public void testCli() throws Exception {
        String tagName = "test-tag-name";
        String rClass = "rcTest";
        String rPeriod = "100";
        String rPeriodEbr = "200";
        String clipName = "Clip";
        String[] args = new String[]{
                "-source", "file:///tmp/foo",
                "-target", "cas-simple:hpp://cas.ip.address?cas.pea",
                "--target-top-tag", tagName,
                "--retention-class", rClass,
                "--retention-period", rPeriod,
                "--ebr-retention-period", rPeriodEbr,
                "--clip-name", clipName
        };

        // use reflection to bootstrap EcsSync using CLI arguments
        Method optionsMethod = EcsSync.class.getDeclaredMethod("cliBootstrap", String[].class);
        optionsMethod.setAccessible(true);
        EcsSync sync = (EcsSync) optionsMethod.invoke(null, (Object) args);

        Object target = sync.getTarget();
        Assert.assertNotNull("target is null", target);
        Assert.assertTrue("target is not CasSimpleTarget", target instanceof CasSimpleTarget);
        CasSimpleTarget csTarget = (CasSimpleTarget) target;

        Assert.assertEquals("retention class mismatch", rClass, csTarget.getRetentionClass());
        Assert.assertEquals("retention period mismatch", Integer.parseInt(rPeriod), csTarget.getRetentionPeriod());
        Assert.assertEquals("ebr retention period mismatch", Integer.parseInt(rPeriodEbr), csTarget.getRetentionPeriodEbr());
        Assert.assertEquals("tag name mismatch", tagName, csTarget.getTopTagName());
        Assert.assertEquals("clip name mismatch", clipName, csTarget.getClipName());
    }

    private void verify(TestObjectSource source, CasSimpleTarget target) throws Exception {
        for (TestSyncObject sourceObject : source) {
            if (!sourceObject.isDirectory()) {
                SyncRecord record = dbService.getSyncRecord((sourceObject));
                String clipId = record.getTargetId();
                verifyClipIntegrity(clipId, sourceObject, target);
            }
        }
    }

    private void verifyClipIntegrity(String clipId, TestSyncObject sourceObject, CasSimpleTarget target) throws Exception {
        FPTag tag = null;
        FPClip clip = null;
        try {
            FPPool pool = new FPPool(connectString);
            String tagName;

            clip = new FPClip(pool, clipId, FPLibraryConstants.FP_OPEN_FLAT);
            tag = clip.getTopTag();
            tagName = tag.getTagName();

            Assert.assertEquals("x-emc-data", tagName);

            if(target.getClipName() != null) {
                Assert.assertEquals("clip name does not match", clipName, clip.getName());
            }

            if (target.getRetentionClass() != null) {
                Assert.assertEquals("retention class does not match", retentionClass, clip.getRetentionClassName());
            }

            if (target.getRetentionPeriodEbr() > 0) {
                Assert.assertEquals("ebr retention period does not match", target.getRetentionPeriodEbr(), clip.getEBRPeriod());
            }

            if (target.getRetentionPeriod() > 0) {
                Assert.assertEquals("retention period does not match", target.getRetentionPeriod(), clip.getRetentionPeriod());
            }

            Assert.assertEquals(1, tag.BlobExists());

            SyncMetadata smd = sourceObject.getMetadata();
            Assert.assertEquals(smd.getContentType(), tag.getStringAttribute("Content-Type"));
            Assert.assertEquals(smd.getContentLength(), Long.parseLong(tag.getStringAttribute("Content-Length")));
            Assert.assertEquals(smd.getModificationTime().getTime(), Long.parseLong(tag.getStringAttribute("x-emc-sync-mtime")));

            Assert.assertEquals(sourceObject.getRelativePath(), tag.getStringAttribute("x-emc-sync-path"));

            for (SyncMetadata.UserMetadata umd : smd.getUserMetadata().values()) {
                String name = umd.getKey();
                String value = umd.getValue();
                tag.Close();
                tag = clip.FetchNext();
                String umd_name = tag.getStringAttribute("name");
                String umd_value = tag.getStringAttribute("value");
                Assert.assertEquals(true, name.equals(umd_name));
                Assert.assertEquals(true, value.equals(umd_value));
            }

        } finally {
            try {
                if (tag != null) tag.Close();
                if (clip != null) clip.Close();
            } catch (FPLibraryException e) {
                // ignore
            }
        }
    }

    private void deleteClips(CasSimpleTarget target, Collection<String> clipIds) {
        try {
            final FPPool pool = new FPPool(target.getConnectionString());

            ExecutorService executor = Executors.newFixedThreadPool(32);
            List<Future> futures = new ArrayList<>();
            for (final String clipId : clipIds) {
                if (clipId != null) {
                    futures.add(executor.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            FPClip.Delete(pool, clipId);
                            return null;
                        }
                    }));
                }
            }

            for (Future future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            pool.Close();
        } catch (FPLibraryException e) {
            e.printStackTrace();
        }
    }
}

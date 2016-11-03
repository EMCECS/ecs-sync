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
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.test.ByteAlteringFilter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class VerifyTest {
    @Test
    public void testSuccess() throws Exception {
        SyncConfig syncConfig = new SyncConfig().withSource(new TestConfig().withObjectCount(1000).withMaxSize(10240).withDiscardData(false))
                .withTarget(new TestConfig().withReadData(true).withDiscardData(false))
                .withOptions(new SyncOptions().withThreadCount(16).withVerify(true));

        // send test data to test system
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.run();

        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
        Assert.assertTrue(sync.getStats().getObjectsComplete() > 1000);

        TestStorage source = (TestStorage) sync.getSource(), target = (TestStorage) sync.getTarget();
        verifyObjects(source, source.getRootObjects(), target, target.getRootObjects(), true);
    }

    @Test
    public void testByteAlteringFilter() throws Exception {
        Random random = new Random();
        byte[] buffer = new byte[1024];
        int totalCount = 10000, errorCount = 0;

        ByteAlteringFilter filter = new ByteAlteringFilter();
        filter.setConfig(new ByteAlteringFilter.ByteAlteringConfig());
        TestStorage target = new TestStorage();
        target.setConfig(new TestConfig().withReadData(true).withDiscardData(false));
        filter.setNext(new TargetFilter(target));

        for (int i = 0; i < totalCount; i++) {
            random.nextBytes(buffer);
            String id = "foo" + i;
            SyncObject object = new SyncObject(target, id, new ObjectMetadata().withContentLength(buffer.length),
                    new ByteArrayInputStream(buffer), null);
            target.createObject(object);
            String originalMd5 = object.getMd5Hex(true);
            SyncObject newObject = filter.reverseFilter(new ObjectContext().withSourceSummary(
                    new ObjectSummary(id, false, buffer.length)).withObject(object));
            String newMd5 = newObject.getMd5Hex(true);
            if (!originalMd5.equals(newMd5)) {
                errorCount++;
            }
        }

        Assert.assertTrue(errorCount > 0);
        Assert.assertNotEquals(errorCount, totalCount);
        Assert.assertEquals(errorCount, filter.getConfig().getModifiedObjects());
    }

    @Test
    public void testFailures() throws Exception {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setSource(new TestConfig().withObjectCount(1000).withMaxSize(10240).withDiscardData(false));
        syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));

        ByteAlteringFilter.ByteAlteringConfig alteringConfig = new ByteAlteringFilter.ByteAlteringConfig();
        syncConfig.setFilters(Collections.singletonList(alteringConfig));

        // retry would circumvent our test
        syncConfig.setOptions(new SyncOptions().withThreadCount(16).withVerify(true).withRetryAttempts(0));

        // send test data to test system
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.run();

        Assert.assertEquals(alteringConfig.getModifiedObjects(), sync.getStats().getObjectsFailed());
    }

    @Test
    public void testVerifyOnly() throws Exception {
        TestStorage source = new TestStorage();
        source.setConfig(new TestConfig().withObjectCount(1000).withMaxSize(10240).withDiscardData(false));
        source.configure(source, null, null); // generates objects

        TestStorage target = new TestStorage();
        target.setConfig(new TestConfig().withReadData(true).withDiscardData(false));
        // must pre-ingest objects to the target so we have something to verify against
        target.ingest(source, null);

        ByteAlteringFilter.ByteAlteringConfig alteringConfig = new ByteAlteringFilter.ByteAlteringConfig();

        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setFilters(Collections.singletonList(alteringConfig));
        syncConfig.setOptions(new SyncOptions().withThreadCount(16).withVerifyOnly(true));

        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setSyncConfig(syncConfig);
        sync.run();

        Assert.assertTrue(alteringConfig.getModifiedObjects() > 0);
        Assert.assertNotEquals(alteringConfig.getModifiedObjects(), sync.getEstimatedTotalObjects());
        Assert.assertEquals(alteringConfig.getModifiedObjects(), sync.getStats().getObjectsFailed());
    }

    public static void verifyObjects(TestStorage source, List<TestStorage.TestSyncObject> sourceObjects,
                                     TestStorage target, List<TestStorage.TestSyncObject> targetObjects,
                                     boolean verifyAcl) {
        for (TestStorage.TestSyncObject sourceObject : sourceObjects) {
            String currentPath = sourceObject.getRelativePath();
            Assert.assertTrue(currentPath + " - missing from target", targetObjects.contains(sourceObject));
            for (TestStorage.TestSyncObject targetObject : targetObjects) {
                if (sourceObject.getRelativePath().equals(targetObject.getRelativePath())) {
                    verifyMetadata(sourceObject.getMetadata(), targetObject.getMetadata(), currentPath);
                    if (verifyAcl) verifyAcl(sourceObject.getAcl(), targetObject.getAcl());
                    if (sourceObject.getMetadata().isDirectory()) {
                        Assert.assertTrue(currentPath + " - source is directory but target is not", targetObject.getMetadata().isDirectory());
                        verifyObjects(source, source.getChildren(source.getIdentifier(sourceObject.getRelativePath(), true)),
                                target, target.getChildren(target.getIdentifier(targetObject.getRelativePath(), true)), verifyAcl);
                    } else {
                        Assert.assertFalse(currentPath + " - source is data object but target is not", targetObject.getMetadata().isDirectory());
                        Assert.assertEquals(currentPath + " - content-type different", sourceObject.getMetadata().getContentType(),
                                targetObject.getMetadata().getContentType());
                        Assert.assertEquals(currentPath + " - data size different", sourceObject.getMetadata().getContentLength(),
                                targetObject.getMetadata().getContentLength());
                        Assert.assertArrayEquals(currentPath + " - data not equal", sourceObject.getData(), targetObject.getData());
                    }
                }
            }
        }
    }

    private static void verifyMetadata(ObjectMetadata sourceMetadata, ObjectMetadata targetMetadata, String path) {
        if (sourceMetadata == null || targetMetadata == null)
            Assert.fail(String.format("%s - metadata can never be null (source: %s, target: %s)",
                    path, sourceMetadata, targetMetadata));

        // must be reasonable about mtime; we can't always set it on the target
        if (sourceMetadata.getModificationTime() == null)
            Assert.assertNull(path + " - source mtime is null, but target is not", targetMetadata.getModificationTime());
        else if (targetMetadata.getModificationTime() == null)
            Assert.fail(path + " - target mtime is null, but source is not");
        else
            Assert.assertTrue(path + " - target mtime is older",
                    sourceMetadata.getModificationTime().compareTo(targetMetadata.getModificationTime()) < 1000);
        Assert.assertEquals(path + " - different user metadata count", sourceMetadata.getUserMetadata().size(),
                targetMetadata.getUserMetadata().size());
        for (String key : sourceMetadata.getUserMetadata().keySet()) {
            Assert.assertEquals(path + " - meta[" + key + "] different", sourceMetadata.getUserMetadataValue(key).trim(),
                    targetMetadata.getUserMetadataValue(key).trim()); // some systems trim metadata values
        }

        // not verifying system metadata here
    }

    private static void verifyAcl(ObjectAcl sourceAcl, ObjectAcl targetAcl) {
        // only verify ACL if it's set on the source
        if (sourceAcl != null) {
            Assert.assertNotNull(targetAcl);
            Assert.assertEquals(sourceAcl, targetAcl); // ObjectAcl implements .equals()
        }
    }
}

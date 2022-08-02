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
package com.emc.ecs.sync;

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.test.ByteAlteringFilter;
import com.emc.ecs.sync.util.VerifyUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

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

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertTrue(sync.getStats().getObjectsComplete() > 1000);

        TestStorage source = (TestStorage) sync.getSource(), target = (TestStorage) sync.getTarget();
        VerifyUtil.verifyObjects(source, source.getRootObjects(), target, target.getRootObjects(), true);
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

        Assertions.assertEquals(alteringConfig.getModifiedObjects(), sync.getStats().getObjectsFailed());
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

        Assertions.assertTrue(alteringConfig.getModifiedObjects() > 0);
        Assertions.assertNotEquals(alteringConfig.getModifiedObjects(), sync.getEstimatedTotalObjects());
        Assertions.assertEquals(alteringConfig.getModifiedObjects(), sync.getStats().getObjectsFailed());
    }

    @Test
    public void testUseMetadataChecksum() throws Exception {
        String foo = "foo", bar = "bar";
        String fooMd5Base64 = getMd5Base64(foo);
        Checksum fooChecksum = Checksum.fromBase64("MD5", fooMd5Base64);
        String barMd5Base64 = getMd5Base64(bar);
        Checksum barChecksum = Checksum.fromBase64("MD5", barMd5Base64);
        TestStorage storage = new TestStorage();

        SyncOptions syncOptions = new SyncOptions();
        Md5Verifier verifier = new Md5Verifier(syncOptions);

        // 1. data is the same, metadata is different
        ByteArrayInputStream sourceStream = new ByteArrayInputStream(foo.getBytes(StandardCharsets.UTF_8));
        ObjectMetadata sourceMeta = new ObjectMetadata().withContentLength(foo.length()).withChecksum(fooChecksum);
        SyncObject sourceObject = new SyncObject(storage, "object1", sourceMeta, sourceStream, new ObjectAcl());

        ByteArrayInputStream targetStream = new ByteArrayInputStream(foo.getBytes(StandardCharsets.UTF_8));
        // in this object, data checksum will *not* match metadata checksum
        ObjectMetadata targetMeta = new ObjectMetadata().withContentLength(foo.length()).withChecksum(barChecksum);
        SyncObject targetObject = new SyncObject(storage, "object1", targetMeta, targetStream, new ObjectAcl());

        // 1.a. use data checksum verification
        syncOptions.setUseMetadataChecksumForVerification(false);
        // this should succeed
        verifier.verify(sourceObject, targetObject);

        // 1.b. use metadata checksum verification
        syncOptions.setUseMetadataChecksumForVerification(true);
        try {
            // this should fail
            verifier.verify(sourceObject, targetObject);
            Assertions.fail("metadata checksum verification passed, but should have failed");
        } catch (RuntimeException e) {
            Assertions.assertTrue(e.getMessage().contains("MD5 sum mismatch"));
        }

        // 2. data is different, metadata is the same
        sourceStream = new ByteArrayInputStream(foo.getBytes(StandardCharsets.UTF_8));
        sourceMeta = new ObjectMetadata().withContentLength(foo.length()).withChecksum(fooChecksum);
        sourceObject = new SyncObject(storage, "object1", sourceMeta, sourceStream, new ObjectAcl());

        targetStream = new ByteArrayInputStream(bar.getBytes(StandardCharsets.UTF_8));
        // in this object, data checksum will *not* match metadata checksum
        targetMeta = new ObjectMetadata().withContentLength(foo.length()).withChecksum(fooChecksum);
        targetObject = new SyncObject(storage, "object1", targetMeta, targetStream, new ObjectAcl());

        // 2.a. use data checksum verification
        syncOptions.setUseMetadataChecksumForVerification(false);
        try {
            // this should fail
            verifier.verify(sourceObject, targetObject);
            Assertions.fail("data checksum verification passed, but should have failed");
        } catch (RuntimeException e) {
            Assertions.assertTrue(e.getMessage().contains("MD5 sum mismatch"));
        }

        // 2.b. use metadata checksum verification
        syncOptions.setUseMetadataChecksumForVerification(true);
        // this should succeed
        verifier.verify(sourceObject, targetObject);
    }

    private String getMd5Base64(String value) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(value.getBytes(StandardCharsets.UTF_8));
        byte[] digest = md.digest();
        return DatatypeConverter.printBase64Binary(digest);
    }
}

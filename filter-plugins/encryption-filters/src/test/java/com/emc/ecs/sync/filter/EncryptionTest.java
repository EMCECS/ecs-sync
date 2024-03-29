/*
 * Copyright (c) 2014-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.filter;

import com.emc.codec.encryption.KeyProvider;
import com.emc.codec.encryption.KeystoreKeyProvider;
import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.filter.DecryptionConfig;
import com.emc.ecs.sync.config.filter.EncryptionConfig;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.test.TestUtil;
import com.emc.ecs.sync.util.VerifyUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.Collections;

public class EncryptionTest {
    private static final String KEYSTORE_RESOURCE_FILE = "store.jks";
    private static final String KEYSTORE_PASSWORD = "ViPR123";
    private static final String KEY_ALIAS = "atmos-encryption";

    private KeyProvider keyProvider;

    @BeforeEach
    public void setUp() throws Exception {
        // Init keystore
        KeyStore keystore = KeyStore.getInstance("jks");
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(KEYSTORE_RESOURCE_FILE);
        if (in == null) {
            throw new FileNotFoundException(KEYSTORE_RESOURCE_FILE);
        }
        keystore.load(in, KEYSTORE_PASSWORD.toCharArray());

        keyProvider = new KeystoreKeyProvider(keystore, KEYSTORE_PASSWORD.toCharArray(), KEY_ALIAS);
    }

    @Test
    public void testEncryptionDecryption() throws Exception {
        final File tempDir = Files.createTempDirectory("ecs-sync-encryption-test").toFile();
        tempDir.deleteOnExit();

        if (!tempDir.exists() || !tempDir.isDirectory())
            throw new RuntimeException("unable to make temp dir");

        try {

            // encrypt while sending to a temp dir
            EncryptionConfig encConfig = new EncryptionConfig();
            encConfig.setFailIfEncrypted(true);

            // must instantiate the filter so we can use a classpath keystore file
            EncryptionFilter encFilter = new EncryptionFilter();
            encFilter.setConfig(new EncryptionConfig());
            encFilter.setKeyProvider(keyProvider);

            TestConfig testConfig = new TestConfig().withObjectCount(25).withMaxSize(10240).withReadData(true)
                    .withDiscardData(false);

            FilesystemConfig tmpConfig = new FilesystemConfig();
            tmpConfig.setPath(tempDir.getPath());
            tmpConfig.setStoreMetadata(true);

            SyncConfig syncConfig = new SyncConfig();
            syncConfig.setSource(testConfig);
            syncConfig.setTarget(tmpConfig);

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.setFilters(Collections.singletonList((SyncFilter) encFilter));
            TestUtil.run(sync);

            Assertions.assertEquals(sync.getEstimatedTotalObjects(), sync.getStats().getObjectsComplete());
            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

            // keep reference to test source
            TestStorage testSource = (TestStorage) sync.getSource();

            // decrypt while reading back
            DecryptionConfig decConfig = new DecryptionConfig();
            decConfig.setFailIfNotEncrypted(true);

            // must instantiate the filter so we can use a classpath keystore file
            DecryptionFilter decFilter = new DecryptionFilter();
            decFilter.setConfig(decConfig);
            decFilter.setKeyProvider(keyProvider);

            syncConfig = new SyncConfig();
            syncConfig.setSource(tmpConfig);
            syncConfig.setTarget(testConfig);

            sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.setFilters(Collections.singletonList((SyncFilter) decFilter));
            TestUtil.run(sync);

            Assertions.assertEquals(sync.getEstimatedTotalObjects(), sync.getStats().getObjectsComplete());
            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

            TestStorage testTarget = (TestStorage) sync.getTarget();

            // verify everything is the same
            VerifyUtil.verifyObjects(testSource, testSource.getRootObjects(),
                    testTarget, testTarget.getRootObjects(), false);

        } finally {
            recursiveDelete(tempDir);
        }
    }

    private void recursiveDelete(File file) {
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                recursiveDelete(child);
            }
        }
        file.delete();
    }
}

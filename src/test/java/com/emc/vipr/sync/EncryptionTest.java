/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync;

import com.emc.vipr.sync.filter.DecryptionFilter;
import com.emc.vipr.sync.filter.EncryptionFilter;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.source.FilesystemSource;
import com.emc.vipr.sync.target.FilesystemTarget;
import com.emc.vipr.sync.test.TestObjectSource;
import com.emc.vipr.sync.test.TestObjectTarget;
import com.emc.vipr.sync.test.TestSyncObject;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.List;

public class EncryptionTest {
    private static final String KEYSTORE_RESOURCE_FILE = "store.jks";
    private static final String KEYSTORE_PASSWORD = "ViPR123";
    private static final String KEY_ALIAS = "atmos-encryption";

    private KeyStore keystore;

    @Before
    public void setUp() throws Exception {
        // Init keystore
        keystore = KeyStore.getInstance("jks");
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(KEYSTORE_RESOURCE_FILE);
        if (in == null) {
            throw new FileNotFoundException(KEYSTORE_RESOURCE_FILE);
        }
        keystore.load(in, KEYSTORE_PASSWORD.toCharArray());
    }

    @Test
    public void testEncryptionDecription() throws Exception {
        final File tempDir = new File("/tmp/vipr-sync-encryption-test");
        tempDir.mkdir();
        tempDir.deleteOnExit();

        if (!tempDir.exists() || !tempDir.isDirectory())
            throw new RuntimeException("unable to make temp dir");

        TestObjectSource testSource = new TestObjectSource(25, 10240, null);

        try {

            // encrypt while sending to a temp dir
            EncryptionFilter encFilter = new EncryptionFilter();
            encFilter.setKeystore(keystore);
            encFilter.setKeystorePass(KEYSTORE_PASSWORD);
            encFilter.setKeyAlias(KEY_ALIAS);
            encFilter.setFailIfEncrypted(true);

            FilesystemTarget target = new FilesystemTarget();
            target.setTargetRoot(tempDir);

            ViPRSync sync = new ViPRSync();
            sync.setSource(testSource);
            sync.setFilters(Arrays.asList((SyncFilter) encFilter));
            sync.setTarget(target);
            sync.setSyncThreadCount(4);
            sync.run();

            // decrypt while reading back
            FilesystemSource source = new FilesystemSource();
            source.setRootFile(tempDir);

            DecryptionFilter decFilter = new DecryptionFilter();
            decFilter.setKeystore(keystore);
            decFilter.setKeystorePass(KEYSTORE_PASSWORD);
            decFilter.setFailIfNotEncrypted(true);

            TestObjectTarget testTarget = new TestObjectTarget();

            sync = new ViPRSync();
            sync.setSource(source);
            sync.setFilters(Arrays.asList((SyncFilter) decFilter));
            sync.setTarget(testTarget);
            sync.setSyncThreadCount(4);
            sync.run();

            // verify everything is the same
            EndToEndTest.verifyObjects(testSource.getObjects(), testTarget.getRootObjects());

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

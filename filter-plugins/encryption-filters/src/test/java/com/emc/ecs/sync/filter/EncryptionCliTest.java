/*
 * Copyright (c) 2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.AbstractCliTest;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.filter.DecryptionConfig;
import com.emc.ecs.sync.config.filter.EncryptionConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class EncryptionCliTest extends AbstractCliTest {
    @Test
    public void testEncryptDecryptCli() {
        String encKeystore = "/tmp/store.jks";
        String encKeyPass = "foo";
        String encKeyAlias = "bar";
        String decKeystore = "/tmp/shop.jks";
        String decKeyPass = "baz";

        String[] args = new String[]{
                "-source", "file:///tmp",
                "-target", "test:",
                "-filters", "encrypt,decrypt",
                "--encrypt-keystore", encKeystore,
                "--encrypt-keystore-pass", encKeyPass,
                "--encrypt-key-alias", encKeyAlias,
                "--encrypt-force-strong",
                "--fail-if-encrypted",
                "--encrypt-update-mtime",
                "--decrypt-keystore", decKeystore,
                "--decrypt-keystore-pass", decKeyPass,
                "--fail-if-not-encrypted",
                "--decrypt-update-mtime"
        };

        SyncConfig syncConfig = parseSyncConfig(args);

        Iterator<?> filters = syncConfig.getFilters().iterator();

        Object filter = filters.next();
        Assertions.assertTrue(filter instanceof EncryptionConfig, "first filter is not encryption");
        EncryptionConfig encFilter = (EncryptionConfig) filter;
        Assertions.assertEquals(encKeystore, encFilter.getEncryptKeystore(), "enc keystore mismatch");
        Assertions.assertEquals(encKeyPass, encFilter.getEncryptKeystorePass(), "enc keystorePass mismatch");
        Assertions.assertEquals(encKeyAlias, encFilter.getEncryptKeyAlias(), "enc keyAlias mismatch");
        Assertions.assertTrue(encFilter.isEncryptForceStrong(), "enc forceString mismatch");
        Assertions.assertTrue(encFilter.isFailIfEncrypted(), "enc failIfEncrypted mismatch");
        Assertions.assertTrue(encFilter.isEncryptUpdateMtime(), "enc updateMtime mismatch");

        filter = filters.next();
        Assertions.assertTrue(filter instanceof DecryptionConfig, "second filter is not decryption");
        DecryptionConfig decFilter = (DecryptionConfig) filter;
        Assertions.assertEquals(decKeystore, decFilter.getDecryptKeystore(), "dec keystore mismatch");
        Assertions.assertEquals(decKeyPass, decFilter.getDecryptKeystorePass(), "dec keystorePass mismatch");
        Assertions.assertTrue(decFilter.isFailIfNotEncrypted(), "dec failIfNotEncrypted mismatch");
        Assertions.assertTrue(decFilter.isDecryptUpdateMtime(), "dec updateMtime mismatch");
    }
}

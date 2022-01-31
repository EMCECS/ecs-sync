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

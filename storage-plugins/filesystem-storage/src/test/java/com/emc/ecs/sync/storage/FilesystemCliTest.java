/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.AbstractCliTest;
import com.emc.ecs.sync.CliConfig;
import com.emc.ecs.sync.CliHelper;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;

public class FilesystemCliTest extends AbstractCliTest {
    @Test
    public void testFilesystemCli() {
        File sourceFile = new File("/tmp/foo");
        File targetFile = new File("/tmp/bar");
        String[] args = new String[]{
                "-source", "file://" + sourceFile,
                "-target", "file://" + targetFile,
                "--source-use-absolute-path",
                "--target-include-base-dir"
        };

        SyncConfig syncConfig = parseSyncConfig(args);

        Object source = syncConfig.getSource();
        Assertions.assertNotNull(source, "source is null");
        Assertions.assertTrue(source instanceof FilesystemConfig, "source is not FilesystemSource");
        FilesystemConfig fsSource = (FilesystemConfig) source;

        Object target = syncConfig.getTarget();
        Assertions.assertNotNull(target, "target is null");
        Assertions.assertTrue(target instanceof FilesystemConfig, "target is not FilesystemTarget");
        FilesystemConfig fsTarget = (FilesystemConfig) target;

        Assertions.assertEquals(sourceFile.getPath(), fsSource.getPath(), "source file mismatch");
        Assertions.assertTrue(fsSource.isUseAbsolutePath(), "source use-absolute-path should be enabled");
        Assertions.assertEquals(targetFile.getPath(), fsTarget.getPath(), "target file mismatch");
        Assertions.assertTrue(fsTarget.isIncludeBaseDir(), "target include-base-dir should be enabled");
    }

    @Test
    public void testFilesystemCli2() {
        String sourcePath = "/source/path";
        String targetPath = "/target/path";
        String dbFile = "myDbFile", dbConString = "myDbConString";
        String dbTable = "myDbTable", encPassword = "myEncDbPassword";

        int bufferSize = 123456;

        String[] args = new String[]{
                "-source", "file:" + sourcePath,
                "-target", "file:" + targetPath,
                "--db-file", dbFile,
                "--db-connect-string", dbConString,
                "--db-table", dbTable,
                "--db-enc-password", encPassword,
                "--delete-source",
                "--no-monitor-performance",
                "--non-recursive",
                "--remember-failed",
                "--sync-acl",
                "--no-sync-data",
                "--no-sync-metadata",
                "--ignore-invalid-acls",
                "--sync-retention-expiration",
                "--force-sync",
                "--buffer-size", "" + bufferSize
        };

        CliConfig cliConfig = CliHelper.parseCliConfig(args);
        SyncConfig syncConfig = CliHelper.parseSyncConfig(cliConfig, args);

        Object source = syncConfig.getSource();
        Assertions.assertNotNull(source, "source is null");
        Assertions.assertTrue(source instanceof FilesystemConfig, "source is not AtmosSource");

        Object target = syncConfig.getTarget();
        Assertions.assertNotNull(target, "target is null");
        Assertions.assertTrue(target instanceof FilesystemConfig, "target is not AtmosTarget");

        SyncOptions options = syncConfig.getOptions();
        Assertions.assertEquals(dbFile, options.getDbFile());
        Assertions.assertEquals(dbConString, options.getDbConnectString());
        Assertions.assertEquals(dbTable, options.getDbTable());
        Assertions.assertEquals(encPassword, options.getDbEncPassword());
        Assertions.assertTrue(options.isDeleteSource());
        Assertions.assertFalse(options.isMonitorPerformance());
        Assertions.assertFalse(options.isRecursive());
        Assertions.assertTrue(options.isRememberFailed());
        Assertions.assertTrue(options.isSyncAcl());
        Assertions.assertFalse(options.isSyncData());
        Assertions.assertFalse(options.isSyncMetadata());
        Assertions.assertTrue(options.isIgnoreInvalidAcls(), "source ignoreInvalidAcls mismatch");
        Assertions.assertTrue(options.isSyncRetentionExpiration());
        Assertions.assertTrue(options.isForceSync(), "source force mismatch");
        Assertions.assertEquals(bufferSize, options.getBufferSize(), "source bufferSize mismatch");
    }
}

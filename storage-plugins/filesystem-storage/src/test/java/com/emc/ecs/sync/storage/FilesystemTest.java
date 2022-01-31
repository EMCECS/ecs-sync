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

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.file.FilesystemStorage;
import com.emc.ecs.sync.util.Iso8601Util;
import com.emc.ecs.sync.util.RandomInputStream;
import com.emc.ecs.sync.util.SyncUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

public class FilesystemTest {
    private File sourceDir;
    private File targetDir;

    @BeforeEach
    public void setup() throws Exception {
        sourceDir = Files.createTempDirectory("ecs-sync-filesystem-source-test").toFile();
        if (!sourceDir.exists() || !sourceDir.isDirectory()) throw new RuntimeException("unable to make source dir");
        targetDir = Files.createTempDirectory("ecs-sync-filesystem-target-test").toFile();
        if (!targetDir.exists() || !targetDir.isDirectory()) throw new RuntimeException("unable to make target dir");
    }

    @AfterEach
    public void teardown() {
        recursiveDelete(sourceDir);
        recursiveDelete(targetDir);
    }

    private void recursiveDelete(File dir) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) recursiveDelete(file);
            else file.delete();
        }
        dir.delete();
    }

    @Test
    public void testModifiedSince() throws Exception {
        final File tempDir = Files.createTempDirectory("ecs-sync-filesystem-source-test").toFile();
        tempDir.deleteOnExit();

        if (!tempDir.exists() || !tempDir.isDirectory())
            throw new RuntimeException("unable to make temp dir");

        try {
            // write 10 files
            int size = 10 * 1024;
            for (int i = 0; i < 10; i++) {
                SyncUtil.copy(new RandomInputStream(size), new FileOutputStream(new File(tempDir, "file-" + i)), size);
            }

            SyncOptions options = new SyncOptions().withThreadCount(16).withVerify(true);

            // sync 10 files to a test target
            FilesystemConfig fsConfig = new FilesystemConfig();
            fsConfig.setPath(tempDir.getPath());

            TestConfig testConfig = new TestConfig().withReadData(true).withDiscardData(false);

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig().withOptions(options).withSource(fsConfig).withTarget(testConfig));
            sync.run();

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(10, sync.getStats().getObjectsComplete());

            // get time
            Date modifiedSince = new Date();

            // wait a tick to make sure mtime will be newer
            Thread.sleep(1000);

            // write 5 more files
            for (int i = 10; i < 15; i++) {
                SyncUtil.copy(new RandomInputStream(size), new FileOutputStream(new File(tempDir, "file-" + i)), size);
            }

            // sync using modifiedSince
            fsConfig.setModifiedSince(Iso8601Util.format(modifiedSince));

            sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig().withOptions(options).withSource(fsConfig).withTarget(testConfig));
            sync.run();

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(5, sync.getStats().getObjectsComplete());

            TestStorage testStorage = (TestStorage) sync.getTarget();

            for (SyncObject object : testStorage.getRootObjects()) {
                Assertions.assertTrue(object.getRelativePath().matches("^file-1[0-4]$"),
                        "unmodified file was synced: " + object.getRelativePath());
            }
        } finally {
            for (File file : tempDir.listFiles()) {
                file.delete();
            }
            new File(tempDir, ObjectMetadata.METADATA_DIR).delete(); // delete this so the temp dir can go away
        }
    }

    @Test
    public void testExcludeFilter() throws Exception {
        Path file = Paths.get(".");

        FilesystemConfig fsConfig = new FilesystemConfig();
        fsConfig.setPath(file.toString());
        fsConfig.setExcludedPaths(new String[]{"(.*/)?\\.[^/]*", "(.*/)?[^/]*foo[^/]*", "(.*/)?[^/]*\\.bin"});

        FilesystemStorage storage = new FilesystemStorage();
        storage.setConfig(fsConfig);
        storage.configure(storage, null, null);

        DirectoryStream.Filter<Path> filter = storage.getFilter();

        String[] positiveTests = new String[]{"bar.txt", "a.out", "this has spaces", "n.o.t.h.i.n.g"};
        for (String test : positiveTests) {
            Assertions.assertTrue(filter.accept(file.resolve(test)), "filter should have accepted " + test);
        }

        String[] negativeTests = new String[]{".svn", ".snapshots", ".f.o.o", "foo.txt", "ffoobar", "mywarez.bin",
                "in.the.round.bin"};
        for (String test : negativeTests) {
            Assertions.assertFalse(filter.accept(file.resolve(test)), "filter should have rejected " + test);
        }
    }

    @Test
    public void testSingleFile() throws Exception {
        String name = "single-file-test";
        File sFile = new File(sourceDir, name);
        File tFile = new File(targetDir, name);
        int size = 100 * 1024;
        SyncUtil.copy(new RandomInputStream(size), new FileOutputStream(sFile), size);

        FilesystemConfig sConfig = new FilesystemConfig();
        sConfig.setPath(sFile.getAbsolutePath());

        FilesystemConfig tConfig = new FilesystemConfig();
        tConfig.setPath(tFile.getAbsolutePath());

        SyncConfig syncConfig = new SyncConfig().withSource(sConfig).withTarget(tConfig);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);

        sync.run();

        Assertions.assertArrayEquals(Files.readAllBytes(sFile.toPath()), Files.readAllBytes(tFile.toPath()));
    }

    @Test
    public void testRelativeLinkTargets() throws Exception {
        String linkName = "my/link";
        Path sourceLink = Paths.get(sourceDir.toString(), linkName);
        String targetName = "this/link/target";
        Path sourceTarget = Paths.get(sourceDir.toString(), targetName).normalize();

        Files.createDirectories(sourceLink.getParent());
        Files.createDirectories(sourceTarget.getParent());
        Files.createFile(sourceTarget);

        Files.createSymbolicLink(sourceLink, sourceTarget);

        FilesystemConfig sConfig = new FilesystemConfig();
        sConfig.setPath(sourceDir.getAbsolutePath());
        sConfig.setRelativeLinkTargets(true);

        FilesystemConfig tConfig = new FilesystemConfig();
        tConfig.setPath(targetDir.getAbsolutePath());

        SyncConfig syncConfig = new SyncConfig().withSource(sConfig).withTarget(tConfig);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);

        sync.run();

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(5, sync.getStats().getObjectsComplete());
        Assertions.assertEquals("../this/link/target".replace('/', File.separatorChar),
                Files.readSymbolicLink(Paths.get(targetDir.toString(), linkName)).toString());
    }
}

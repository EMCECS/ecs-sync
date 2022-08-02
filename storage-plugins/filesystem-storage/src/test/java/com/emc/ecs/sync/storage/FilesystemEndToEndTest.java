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
package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.AbstractEndToEndTest;
import com.emc.ecs.sync.config.storage.ArchiveConfig;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.ObjectMetadata;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FilesystemEndToEndTest extends AbstractEndToEndTest {
    @Test
    public void testFilesystem() {
        try {
            Path tempDir = Files.createTempDirectory("ecs-sync-filesystem-test");

            FilesystemConfig filesystemConfig = new FilesystemConfig();
            filesystemConfig.setPath(tempDir.toAbsolutePath().toString());
            filesystemConfig.setStoreMetadata(true);

            multiEndToEndTest(filesystemConfig, new TestConfig(), false);

            Files.deleteIfExists(tempDir.resolve(ObjectMetadata.METADATA_DIR));
            Files.deleteIfExists(tempDir);
        } catch (IOException e) {
            throw new RuntimeException("problem with temp dir", e);
        }
    }

    @Test
    public void testArchive() throws IOException {
        // must create unique temporary files, but they cannot exist prior to testing
        final File archive1 = File.createTempFile("ecs-sync-archive-test-1", ".zip");
        final File archive2 = File.createTempFile("ecs-sync-archive-test-2", ".zip");
        archive1.delete();
        archive2.delete();
        archive1.deleteOnExit();
        archive2.deleteOnExit();

        ArchiveConfig archiveConfig = new ArchiveConfig();
        archiveConfig.setPath(archive1.getPath());
        archiveConfig.setStoreMetadata(true);

        TestConfig testConfig = new TestConfig().withReadData(true).withDiscardData(false);
        testConfig.withObjectCount(LG_OBJ_COUNT).withMaxSize(LG_OBJ_MAX_SIZE);

        endToEndTest(archiveConfig, testConfig, null, false, "large object", false);
        archiveConfig.setPath(archive2.getPath());
        endToEndTest(archiveConfig, testConfig, null, false, "extended DB fields", true);
    }
}

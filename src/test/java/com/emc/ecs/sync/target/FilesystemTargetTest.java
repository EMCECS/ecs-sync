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
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.source.FilesystemSource;
import com.emc.ecs.sync.test.RandomInputStream;
import com.emc.ecs.sync.test.TestObjectSource;
import com.emc.ecs.sync.test.TestObjectTarget;
import com.emc.util.StreamUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

public class FilesystemTargetTest {
    @Test
    public void testFileTimes() throws Exception {
        final File tempDir = new File("/tmp/ecs-sync-filesystem-source-test");
        final File targetDir = new File("/tmp/ecs-sync-filesystem-target-test");
        tempDir.mkdir();
        tempDir.deleteOnExit();
        targetDir.mkdir();
        targetDir.deleteOnExit();

        if (!tempDir.exists() || !tempDir.isDirectory())
            throw new RuntimeException("unable to make source dir");
        if (!targetDir.exists() || !targetDir.isDirectory())
            throw new RuntimeException("unable to make target dir");

        try {
            // write 10 files
            int size = 10 * 1024;
            for (int i = 0; i < 10; i++) {
                StreamUtil.copy(new RandomInputStream(size), new FileOutputStream(new File(tempDir, "file-" + i)), size);
            }

            // sync files to a test target
            FilesystemSource fSource = new FilesystemSource();
            fSource.setRootFile(tempDir);

            TestObjectTarget testTarget = new TestObjectTarget();

            EcsSync sync = new EcsSync();
            sync.setSource(fSource);
            sync.setTarget(testTarget);
            sync.setSyncThreadCount(16);
            sync.setVerify(true);
            sync.run();

            Assert.assertEquals(0, sync.getObjectsFailed());
            Assert.assertEquals(11, sync.getObjectsComplete());

            // sync from test target back to filesystem in a separate dir
            TestObjectSource testSource = new TestObjectSource(testTarget.getRootObjects());

            FilesystemTarget fTarget = new FilesystemTarget();
            fTarget.setTargetRoot(targetDir);

            sync = new EcsSync();
            sync.setSource(testSource);
            sync.setTarget(fTarget);
            sync.setSyncThreadCount(16);
            sync.setVerify(false); // verification will screw up atime
            sync.run();

            Assert.assertEquals(0, sync.getObjectsFailed());
            Assert.assertEquals(10, sync.getObjectsComplete());

            for (File sourceFile : tempDir.listFiles()) {
                File targetFile = new File(targetDir, sourceFile.getName());
                BasicFileAttributes attributes = Files.getFileAttributeView(sourceFile.toPath(),
                        BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes();
                FileTime sourceMtime = attributes.lastModifiedTime();
                FileTime sourceAtime = attributes.lastAccessTime();
                FileTime sourceCrtime = attributes.creationTime();
                attributes = Files.getFileAttributeView(targetFile.toPath(),
                        BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes();
                FileTime targetMtime = attributes.lastModifiedTime();
                FileTime targetAtime = attributes.lastAccessTime();
                FileTime targetCrtime = attributes.creationTime();
                Assert.assertEquals(sourceMtime, targetMtime);
                Assert.assertEquals(sourceAtime, targetAtime);
                Assert.assertEquals(sourceCrtime, targetCrtime);
            }
        } finally {
            for (File file : tempDir.listFiles()) {
                file.delete();
            }
            new File(tempDir, SyncMetadata.METADATA_DIR).delete(); // delete this so the temp dir can go away
            for (File file : targetDir.listFiles()) {
                file.delete();
            }
            new File(targetDir, SyncMetadata.METADATA_DIR).delete(); // delete this so the target dir can go away
        }
    }
}

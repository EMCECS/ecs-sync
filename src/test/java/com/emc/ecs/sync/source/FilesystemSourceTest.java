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
package com.emc.ecs.sync.source;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.test.RandomInputStream;
import com.emc.ecs.sync.test.TestObjectTarget;
import com.emc.ecs.sync.test.TestSyncObject;
import com.emc.ecs.sync.util.FilesystemUtil;
import com.emc.ecs.sync.util.Iso8601Util;
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
import java.util.Date;

public class FilesystemSourceTest {
    @Test
    public void testModifiedSince() throws Exception {
        final File tempDir = File.createTempFile("ecs-sync-filesystem-test", null);
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();

        if (!tempDir.exists() || !tempDir.isDirectory())
            throw new RuntimeException("unable to make temp dir");

        try {
            // write 10 files
            int size = 10 * 1024;
            for (int i = 0; i < 10; i++) {
                StreamUtil.copy(new RandomInputStream(size), new FileOutputStream(new File(tempDir, "file-" + i)), size);
            }

            // sync 10 files to a test target
            FilesystemSource source = new FilesystemSource();
            source.setRootFile(tempDir);

            TestObjectTarget target = new TestObjectTarget();

            EcsSync sync = new EcsSync();
            sync.setSource(source);
            sync.setTarget(target);
            sync.setSyncThreadCount(16);
            sync.setVerify(true);
            sync.run();

            Assert.assertEquals(0, sync.getObjectsFailed());
            Assert.assertEquals(11, sync.getObjectsComplete());

            // get time
            Date modifiedSince = new Date();

            // wait a tick to make sure mtime will be newer
            Thread.sleep(1000);

            // write 5 more files
            for (int i = 10; i < 15; i++) {
                StreamUtil.copy(new RandomInputStream(size), new FileOutputStream(new File(tempDir, "file-" + i)), size);
            }

            // sync using modifiedSince
            source.setModifiedSince(modifiedSince);
            target = new TestObjectTarget();

            sync = new EcsSync();
            sync.setSource(source);
            sync.setTarget(target);
            sync.setSyncThreadCount(16);
            sync.setVerify(true);
            sync.run();

            Assert.assertEquals(0, sync.getObjectsFailed());
            Assert.assertEquals(6, sync.getObjectsComplete());

            for (TestSyncObject object : target.getRootObjects()) {
                Assert.assertTrue("unmodified file was synced: " + object.getRelativePath(),
                        object.getRelativePath().matches("^file-1[0-4]$"));
            }
        } finally {
            for (File file : tempDir.listFiles()) {
                file.delete();
            }
            new File(tempDir, SyncMetadata.METADATA_DIR).delete(); // delete this so the temp dir can go away
        }
    }

    @Test
    public void testFileTimes() throws Exception {
        final File tempDir = File.createTempFile("ecs-sync-filesystem-source-test", null);
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();

        if (!tempDir.exists() || !tempDir.isDirectory())
            throw new RuntimeException("unable to make temp dir");

        try {
            // write 10 files
            int size = 10 * 1024;
            for (int i = 0; i < 10; i++) {
                StreamUtil.copy(new RandomInputStream(size), new FileOutputStream(new File(tempDir, "file-" + i)), size);
            }

            // sync 10 files to a test target
            FilesystemSource source = new FilesystemSource();
            source.setRootFile(tempDir);

            TestObjectTarget target = new TestObjectTarget();

            EcsSync sync = new EcsSync();
            sync.setSource(source);
            sync.setTarget(target);
            sync.setSyncThreadCount(16);
            sync.setVerify(true);
            sync.run();

            Assert.assertEquals(0, sync.getObjectsFailed());
            Assert.assertEquals(11, sync.getObjectsComplete());

            for (TestSyncObject object : target.getRootObjects()) {
                File file = new File(tempDir, object.getRelativePath());
                BasicFileAttributes attributes = Files.getFileAttributeView(file.toPath(), BasicFileAttributeView.class,
                        LinkOption.NOFOLLOW_LINKS).readAttributes();
                FileTime mtime = attributes.lastModifiedTime();
                FileTime atime = attributes.lastAccessTime();
                FileTime crtime = attributes.creationTime();
                Date mtimeDate = Iso8601Util.parse(object.getMetadata().getUserMetadataValue(FilesystemUtil.META_MTIME));
                if (mtime == null) Assert.assertNull(mtimeDate);
                else Assert.assertEquals(mtime.toMillis(), mtimeDate.getTime());
                Date atimeDate = Iso8601Util.parse(object.getMetadata().getUserMetadataValue(FilesystemUtil.META_ATIME));
                if (atime == null) Assert.assertNull(atimeDate);
                else Assert.assertEquals(atime.toMillis(), atimeDate.getTime());
                Date crtimeDate = Iso8601Util.parse(object.getMetadata().getUserMetadataValue(FilesystemUtil.META_CRTIME));
                if (crtime == null) Assert.assertNull(crtimeDate);
                else Assert.assertEquals(crtime.toMillis(), crtimeDate.getTime());
            }
        } finally {
            for (File file : tempDir.listFiles()) {
                file.delete();
            }
            new File(tempDir, SyncMetadata.METADATA_DIR).delete(); // delete this so the temp dir can go away
        }
    }
}

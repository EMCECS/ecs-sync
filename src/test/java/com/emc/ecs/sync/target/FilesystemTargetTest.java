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
import com.emc.ecs.sync.source.FilesystemSource;
import com.emc.ecs.sync.test.RandomInputStream;
import com.emc.ecs.sync.test.TestObjectSource;
import com.emc.ecs.sync.test.TestObjectTarget;
import com.emc.util.StreamUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FilesystemTargetTest {
    Logger log = LoggerFactory.getLogger(FilesystemTargetTest.class);

    File sourceDir;
    File targetDir;

    @Before
    public void setup() throws Exception {
        sourceDir = Files.createTempDirectory("ecs-sync-filesystem-source-test").toFile();
        if (!sourceDir.exists() || !sourceDir.isDirectory()) throw new RuntimeException("unable to make source dir");
        targetDir = Files.createTempDirectory("ecs-sync-filesystem-target-test").toFile();
        if (!targetDir.exists() || !targetDir.isDirectory()) throw new RuntimeException("unable to make target dir");
    }

    @After
    public void teardown() throws Exception {
        for (File file : sourceDir.listFiles()) {
            file.delete();
        }
        sourceDir.delete();
        for (File file : targetDir.listFiles()) {
            file.delete();
        }
        targetDir.delete();
    }

    @Test
    public void testFileTimes() throws Exception {
        // write 10 files
        int size = 10 * 1024;
        for (int i = 0; i < 10; i++) {
            StreamUtil.copy(new RandomInputStream(size), new FileOutputStream(new File(sourceDir, "file-" + i)), size);
        }

        // sync files to a test target
        FilesystemSource fSource = new FilesystemSource();
        fSource.setRootFile(sourceDir);

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
        fTarget.setIgnoreMetadata(true);

        sync = new EcsSync();
        sync.setSource(testSource);
        sync.setTarget(fTarget);
        sync.setSyncThreadCount(16);
        sync.setVerify(false); // verification will screw up atime
        sync.run();

        Assert.assertEquals(0, sync.getObjectsFailed());
        Assert.assertEquals(10, sync.getObjectsComplete());

        for (File sourceFile : sourceDir.listFiles()) {
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
    }

    @Test
    public void testPosixAclPreservation() throws Exception {
        // can only change ownership if root
        boolean isRoot = "root".equals(System.getProperty("user.name"));
        if (isRoot) log.warn("detected root execution");

        int uid = 1111, gid = 2222;
        Set<PosixFilePermission> permissions = new HashSet<>(Arrays.asList(PosixFilePermission.OWNER_READ,
                PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE));

        // write 10 files
        writeTestFiles(sourceDir, 10, 10 * 1024);

        // set uid/gid/mode on files
        for (File file : sourceDir.listFiles()) {
            if (isRoot) Files.setAttribute(file.toPath(), "unix:uid", uid);
            if (isRoot) Files.setAttribute(file.toPath(), "unix:gid", gid);
            Files.setPosixFilePermissions(file.toPath(), permissions);
        }

        // sync files to a test target
        FilesystemSource fSource = new FilesystemSource();
        fSource.setRootFile(sourceDir);

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
        fTarget.setIgnoreMetadata(true);

        sync = new EcsSync();
        sync.setSource(testSource);
        sync.setTarget(fTarget);
        sync.setSyncThreadCount(16);
        sync.setVerify(false); // verification will screw up atime
        sync.run();

        Assert.assertEquals(0, sync.getObjectsFailed());
        Assert.assertEquals(10, sync.getObjectsComplete());

        for (File sourceFile : sourceDir.listFiles()) {
            File targetFile = new File(targetDir, sourceFile.getName());
            if (isRoot) Assert.assertEquals(uid, Files.getAttribute(targetFile.toPath(), "unix:uid"));
            if (isRoot) Assert.assertEquals(gid, Files.getAttribute(targetFile.toPath(), "unix:gid"));
            Assert.assertEquals(permissions, Files.getPosixFilePermissions(targetFile.toPath()));
        }
    }

    private void writeTestFiles(File dir, int count, int size) throws IOException {
        for (int i = 0; i < count; i++) {
            StreamUtil.copy(new RandomInputStream(size), new FileOutputStream(new File(dir, "file-" + i)), size);
        }
    }
}

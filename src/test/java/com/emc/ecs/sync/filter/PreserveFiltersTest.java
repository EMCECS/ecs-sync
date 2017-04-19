/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.filter.PreserveAclConfig;
import com.emc.ecs.sync.config.filter.PreserveFileAttributesConfig;
import com.emc.ecs.sync.config.filter.RestoreAclConfig;
import com.emc.ecs.sync.config.filter.RestoreFileAttributesConfig;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.util.Iso8601Util;
import com.emc.ecs.sync.util.RandomInputStream;
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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

public class PreserveFiltersTest {
    private static final Logger log = LoggerFactory.getLogger(PreserveFiltersTest.class);
    private File sourceDir;
    private File targetDir;

    @Before
    public void setup() throws Exception {
        sourceDir = Files.createTempDirectory("ecs-sync-preserve-filter-test-source").toFile();
        if (!sourceDir.exists() || !sourceDir.isDirectory()) throw new RuntimeException("unable to make source dir");
        targetDir = Files.createTempDirectory("ecs-sync-preserve-filter-test-target").toFile();
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
    public void testPosixAclPreservation() throws Exception {
        // can only change ownership if root
        boolean isRoot = "root".equals(System.getProperty("user.name"));
        if (isRoot) log.warn("detected root execution");

        int uid = 1111, gid = 2222;
        Set<PosixFilePermission> permissions = new HashSet<>(Arrays.asList(PosixFilePermission.OWNER_READ,
                PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE));

        // write 10 files
        writeTestFiles(sourceDir, 10, 10 * 1024, uid, gid, permissions);

        SyncOptions options = new SyncOptions().withThreadCount(16).withVerify(true);

        // sync files to a test target
        FilesystemConfig fsConfig = new FilesystemConfig();
        fsConfig.setPath(sourceDir.getPath());
        fsConfig.setStoreMetadata(true);

        TestConfig testConfig = new TestConfig().withReadData(true).withDiscardData(false);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options).withSource(fsConfig).withTarget(testConfig)
                .withFilters(Collections.singletonList(new PreserveAclConfig())));
        sync.run();

        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
        Assert.assertEquals(10, sync.getStats().getObjectsComplete());

        // sync from test target back to filesystem in a separate dir
        TestStorage testStorage = (TestStorage) sync.getTarget();

        fsConfig.setPath(targetDir.getPath());

        options.setVerify(false); // verification will screw up atime
        options.setSyncAcl(true);
        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options).withTarget(fsConfig)
                .withFilters(Collections.singletonList(new RestoreAclConfig())));
        sync.setSource(testStorage);
        sync.run();

        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
        Assert.assertEquals(10, sync.getStats().getObjectsComplete());

        for (File sourceFile : sourceDir.listFiles()) {
            File targetFile = new File(targetDir, sourceFile.getName());
            if (isRoot) {
                Assert.assertEquals(uid, Files.getAttribute(targetFile.toPath(), "unix:uid"));
                Assert.assertEquals(gid, Files.getAttribute(targetFile.toPath(), "unix:gid"));
            }
            Assert.assertEquals(permissions, Files.getPosixFilePermissions(targetFile.toPath()));
        }
    }

    @Test
    public void testPreserveFileAttributes() throws Exception {
        // can only change ownership if root
        boolean isRoot = "root".equals(System.getProperty("user.name"));
        if (isRoot) log.warn("detected root execution");

        Integer uid = 1111, gid = 2222;
        Set<PosixFilePermission> permissions = new HashSet<>(Arrays.asList(PosixFilePermission.OWNER_READ,
                PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE));
        String mode = "r-----rwx"; // matches permissions above

        // write 10 files
        writeTestFiles(sourceDir, 10, 10 * 1024, uid, gid, permissions);

        SyncOptions options = new SyncOptions().withThreadCount(16);

        // sync 10 files to a test target
        FilesystemConfig fsConfig = new FilesystemConfig();
        fsConfig.setPath(sourceDir.getPath());

        TestConfig testConfig = new TestConfig().withReadData(true).withDiscardData(false);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options).withSource(fsConfig).withTarget(testConfig)
                .withFilters(Collections.singletonList(new PreserveFileAttributesConfig())));
        sync.run();

        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
        Assert.assertEquals(10, sync.getStats().getObjectsComplete());

        TestStorage testStorage = (TestStorage) sync.getTarget();

        for (SyncObject object : testStorage.getRootObjects()) {
            File file = new File(sourceDir, object.getRelativePath());
            BasicFileAttributes attributes = Files.getFileAttributeView(file.toPath(), BasicFileAttributeView.class,
                    LinkOption.NOFOLLOW_LINKS).readAttributes();
            FileTime mtime = attributes.lastModifiedTime();
            FileTime atime = attributes.lastAccessTime();
            FileTime crtime = attributes.creationTime();
            Date mtimeDate = Iso8601Util.parse(object.getMetadata().getUserMetadataValue(PreserveFilters.META_MTIME));
            if (mtime == null) Assert.assertNull(mtimeDate);
            else Assert.assertEquals(mtime.toMillis(), mtimeDate.getTime());
            Date atimeDate = Iso8601Util.parse(object.getMetadata().getUserMetadataValue(PreserveFilters.META_ATIME));
            if (atime == null) Assert.assertNull(atimeDate);
            else // atime is affected by reading the file times
                Assert.assertTrue(Math.abs(atime.toMillis() - atimeDate.getTime()) < 1000);
            Date crtimeDate = Iso8601Util.parse(object.getMetadata().getUserMetadataValue(PreserveFilters.META_CRTIME));
            if (crtime == null) Assert.assertNull(crtimeDate);
            else Assert.assertEquals(crtime.toMillis(), crtimeDate.getTime());

            // check permissions
            if (isRoot) {
                Assert.assertEquals(uid.toString(), object.getMetadata().getUserMetadataValue(PreserveFilters.META_POSIX_UID));
                Assert.assertEquals(gid.toString(), object.getMetadata().getUserMetadataValue(PreserveFilters.META_POSIX_GID));
            }
            Assert.assertEquals(mode, object.getMetadata().getUserMetadataValue(PreserveFilters.META_POSIX_MODE));
        }
    }

    @Test
    public void testRestoreFileAttributes() throws Exception {
        // can only change ownership if root
        boolean isRoot = "root".equals(System.getProperty("user.name"));
        if (isRoot) log.warn("detected root execution");

        // write 10 files
        Integer uid = 1111, gid = 2222;
        Set<PosixFilePermission> permissions = new HashSet<>(Arrays.asList(PosixFilePermission.OWNER_READ,
                PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE));

        // write 10 files
        writeTestFiles(sourceDir, 10, 10 * 1024, uid, gid, permissions);

        SyncOptions options = new SyncOptions().withThreadCount(16).withVerify(true);

        // sync files to a test target
        FilesystemConfig fsConfig = new FilesystemConfig();
        fsConfig.setPath(sourceDir.getPath());

        TestConfig testConfig = new TestConfig().withReadData(true).withDiscardData(false);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options).withSource(fsConfig).withTarget(testConfig)
                .withFilters(Collections.singletonList(new PreserveFileAttributesConfig())));
        sync.run();

        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
        Assert.assertEquals(10, sync.getStats().getObjectsComplete());

        // wait a tick to make sure mtimes are different
        Thread.sleep(1000);

        TestStorage testStorage = (TestStorage) sync.getTarget();

        // sync from test target back to filesystem in a separate dir
        fsConfig.setPath(targetDir.getPath());

        options.setVerify(false); // verification will screw up atime
        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withOptions(options).withTarget(fsConfig)
                .withFilters(Collections.singletonList(new RestoreFileAttributesConfig())));
        sync.setSource(testStorage);
        sync.run();

        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
        Assert.assertEquals(10, sync.getStats().getObjectsComplete());

        for (File sourceFile : sourceDir.listFiles()) {
            File targetFile = new File(targetDir, sourceFile.getName());
            BasicFileAttributes attributes = Files.getFileAttributeView(sourceFile.toPath(),
                    BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes();
            // java's time parsing is only millisecond-accurate
            FileTime sourceMtime = FileTime.fromMillis(attributes.lastModifiedTime().toMillis());
            FileTime sourceAtime = FileTime.fromMillis(attributes.lastAccessTime().toMillis());
            FileTime sourceCrtime = FileTime.fromMillis(attributes.creationTime().toMillis());
            attributes = Files.getFileAttributeView(targetFile.toPath(),
                    BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes();
            FileTime targetMtime = attributes.lastModifiedTime();
            FileTime targetAtime = attributes.lastAccessTime();
            FileTime targetCrtime = attributes.creationTime();
            Assert.assertEquals(sourceMtime, targetMtime);
            // atime is affected by reading the file times
            Assert.assertTrue(Math.abs(sourceAtime.toMillis() - targetAtime.toMillis()) < 1000);
            Assert.assertEquals(sourceCrtime, targetCrtime);

            // check permissions
            if (isRoot) Assert.assertEquals(uid, Files.getAttribute(targetFile.toPath(), "unix:uid"));
            if (isRoot) Assert.assertEquals(gid, Files.getAttribute(targetFile.toPath(), "unix:gid"));
            Assert.assertEquals(permissions, Files.getPosixFilePermissions(targetFile.toPath()));
        }
    }

    @Test
    public void testRestoreSymLink() throws Exception {
        String file = "concrete-file", link = "sym-link";
        int gid = 10;
        // write concrete file
        try (OutputStream out = new FileOutputStream(new File(sourceDir, file))) {
            StreamUtil.copy(new RandomInputStream(1024), out, 1024);
        }

        Files.createSymbolicLink(Paths.get(sourceDir.getPath(), link), Paths.get(file));

        Files.setAttribute(Paths.get(sourceDir.getPath(), file), "unix:gid", gid, LinkOption.NOFOLLOW_LINKS);
        Files.setAttribute(Paths.get(sourceDir.getPath(), link), "unix:gid", gid, LinkOption.NOFOLLOW_LINKS);

        Date fileMtime = new Date(Files.getLastModifiedTime(Paths.get(sourceDir.getPath(), file)).toMillis());
        Date linkMtime = new Date(Files.getLastModifiedTime(Paths.get(sourceDir.getPath(), link), LinkOption.NOFOLLOW_LINKS).toMillis());

        Thread.sleep(5000);

        FilesystemConfig fsConfig = new FilesystemConfig();
        fsConfig.setPath(sourceDir.getPath());
        TestConfig testConfig = new TestConfig().withReadData(true).withDiscardData(false);
        PreserveFileAttributesConfig preserveConfig = new PreserveFileAttributesConfig();

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withSource(fsConfig).withTarget(testConfig)
                .withFilters(Collections.singletonList(preserveConfig)));
        sync.run();

        TestStorage testStorage = (TestStorage) sync.getTarget();

        Assert.assertEquals(2, sync.getStats().getObjectsComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        fsConfig.setPath(targetDir.getPath());

        sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withTarget(fsConfig)
                .withFilters(Collections.singletonList(new RestoreFileAttributesConfig())));
        sync.setSource(testStorage);
        sync.run();

        Assert.assertEquals(2, sync.getStats().getObjectsComplete());
        Assert.assertEquals(0, sync.getStats().getObjectsFailed());

        Thread.sleep(2000); // make sure cache is settled (avoid stale attributes)

        Assert.assertEquals(gid, Files.getAttribute(Paths.get(targetDir.getPath(), file), "unix:gid", LinkOption.NOFOLLOW_LINKS));
        Assert.assertEquals(gid, Files.getAttribute(Paths.get(targetDir.getPath(), link), "unix:gid", LinkOption.NOFOLLOW_LINKS));

        Date tFileMtime = new Date(Files.getLastModifiedTime(Paths.get(targetDir.getPath(), file)).toMillis());
        Date tLinkMtime = new Date(Files.getLastModifiedTime(Paths.get(targetDir.getPath(), link), LinkOption.NOFOLLOW_LINKS).toMillis());

        Assert.assertEquals(fileMtime, tFileMtime);
//        Assert.assertEquals(linkMtime, tLinkMtime); // TODO: figure out a way to set mtime on a link in Java
    }

    private void writeTestFiles(File dir, int count, int size, int uid, int gid, Set<PosixFilePermission> permissions)
            throws IOException {
        for (int i = 0; i < count; i++) {
            File file = new File(dir, "file-" + i);
            StreamUtil.copy(new RandomInputStream(size), new FileOutputStream(file), size);
            // set uid/gid/mode
            if ("root".equals(System.getProperty("user.name"))) {
                Files.setAttribute(file.toPath(), "unix:uid", uid);
                Files.setAttribute(file.toPath(), "unix:gid", gid);
            }
            Files.setPosixFilePermissions(file.toPath(), permissions);
        }
    }
}

/*
 * Copyright (c) 2017-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.nfsclient.nfs.Nfs;
import com.emc.ecs.nfsclient.nfs.NfsException;
import com.emc.ecs.nfsclient.nfs.io.NfsFile;
import com.emc.ecs.nfsclient.nfs.io.NfsFileInputStream;
import com.emc.ecs.nfsclient.nfs.io.NfsFileOutputStream;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.emc.ecs.nfsclient.rpc.CredentialUnix;
import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
import com.emc.ecs.sync.config.storage.NfsConfig;
import com.emc.ecs.sync.storage.nfs.Nfs3Storage;
import com.emc.ecs.sync.test.TestUtil;
import com.emc.ecs.sync.util.RandomInputStream;
import com.emc.ecs.sync.util.SyncUtil;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Properties;

public class NfsTest {
    private static final Logger log = LoggerFactory.getLogger(NfsTest.class);

    private Nfs nfs;
    private String testDirectoryPath = "/testDir";
    private String sourceDirectoryName = "sourceDir";
    private String targetDirectoryName = "targetDir";
    private String testFileName = "testFile";

    private NfsFile testDirectory;
    private NfsFile sourceDirectory;
    private NfsFile sourceFile;
    private NfsFile targetDirectory;
    private NfsFile targetFile;

    private File filesystemSourceDirectory;
    private File filesystemSourceFile;
    private File filesystemTargetDirectory;
    private File filesystemTargetFile;

    @BeforeEach
    public void setup() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();
        String export = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_NFS_EXPORT);
        Assumptions.assumeTrue(export != null);
        if (!export.contains(":")) throw new RuntimeException("invalid export: " + export);
        String server = export.split(":")[0];
        String mountPath = export.split(":")[1];

        nfs = new Nfs3(server, mountPath, new CredentialUnix(0, 0, null), 3);
        testDirectory = nfs.newFile(testDirectoryPath);
        try {
            testDirectory.mkdir();
        } catch (NfsException e) {
            log.error("NFS error code: {}", e.getStatus().getValue());
            throw e;
        }

        sourceDirectory = testDirectory.getChildFile(sourceDirectoryName);
        sourceDirectory.mkdir();
        sourceFile = sourceDirectory.getChildFile(testFileName);
        sourceFile.createNewFile();
        int size = 100 * 1024;
        NfsFileOutputStream outputStream = new NfsFileOutputStream(sourceFile);
        try {
            SyncUtil.copy(new RandomInputStream(size), outputStream, size);
        } finally {
            try {
                outputStream.flush();
            } catch (Exception e) {
                // do nothing
            }
            try {
                outputStream.close();
            } catch (Exception e) {
                // do nothing
            }
        }

        targetDirectory = testDirectory.getChildFile(targetDirectoryName);
        targetDirectory.mkdir();
        targetFile = targetDirectory.getChildFile(testFileName);

        filesystemSourceDirectory = Files.createTempDirectory("ecs-sync-filesystem-source-test").toFile();
        if (!filesystemSourceDirectory.exists() || !filesystemSourceDirectory.isDirectory()) {
            throw new RuntimeException("unable to make filesystem source directory");
        }

        filesystemSourceFile = new File(filesystemSourceDirectory, testFileName);
        SyncUtil.copy(new RandomInputStream(size), new FileOutputStream(filesystemSourceFile), size);

        filesystemTargetDirectory = Files.createTempDirectory("ecs-sync-filesystem-target-test").toFile();
        if (!filesystemTargetDirectory.exists() || !filesystemTargetDirectory.isDirectory()) {
            throw new RuntimeException("unable to make filesystem target directory");
        }

        filesystemTargetFile = new File(filesystemTargetDirectory, testFileName);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (targetFile != null) deleteFile(targetFile);
        if (targetDirectory != null) deleteFile(targetDirectory);
        if (sourceFile != null) deleteFile(sourceFile);
        if (sourceDirectory != null) deleteFile(sourceDirectory);
        if (testDirectory != null) deleteFile(testDirectory);
        if (filesystemSourceDirectory != null) {
            for (File file : filesystemSourceDirectory.listFiles()) {
                file.delete();
            }
            filesystemSourceDirectory.delete();
        }
        if (filesystemTargetDirectory != null) {
            for (File file : filesystemTargetDirectory.listFiles()) {
                file.delete();
            }
            filesystemTargetDirectory.delete();
        }
    }

    /**
     * @param nfsFile
     */
    private void deleteFile(NfsFile nfsFile) {
        try {
            if (nfsFile.exists()) {
                nfsFile.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testNullSubPath() throws Exception {
        NfsConfig sourceConfig = getNfsConfig(nfs);
        Assertions.assertNull(sourceConfig.getSubPath());
        Nfs3Storage nfsStorage = new Nfs3Storage();
        nfsStorage.setConfig(sourceConfig);
        try {
            nfsStorage.configure(nfsStorage, Collections.emptyIterator(), nfsStorage);
        } catch (Throwable t) {
            t.printStackTrace();
            Assertions.fail("This should not throw an exception");
        }
    }

    @Test
    public void testSingleFile() throws Exception {
        NfsConfig sourceConfig = getNfsConfig(nfs);
        sourceConfig.setSubPath(sourceDirectoryName + NfsFile.separator + testFileName);

        NfsConfig targetConfig = getNfsConfig(nfs);
        targetConfig.setSubPath(targetDirectoryName + NfsFile.separator + testFileName);

        SyncConfig syncConfig = new SyncConfig().withSource(sourceConfig).withTarget(targetConfig);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);

        TestUtil.run(sync);

        byte[] sourceBytes = readBytes(sourceFile);
        byte[] targetBytes = readBytes(targetFile);
        Assertions.assertArrayEquals(sourceBytes, targetBytes);
    }

    @Test
    public void testSingleFileFromFilesystem() throws Exception {
        FilesystemConfig sourceConfig = new FilesystemConfig();
        sourceConfig.setPath(filesystemSourceFile.getAbsolutePath());

        NfsConfig targetConfig = getNfsConfig(nfs);
        targetConfig.setSubPath(targetDirectoryName + NfsFile.separator + testFileName);

        SyncConfig syncConfig = new SyncConfig().withSource(sourceConfig).withTarget(targetConfig);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);

        TestUtil.run(sync);

        byte[] sourceBytes = Files.readAllBytes(filesystemSourceFile.toPath());
        byte[] targetBytes = readBytes(targetFile);
        Assertions.assertArrayEquals(sourceBytes, targetBytes);
    }

    @Test
    public void testSingleFileToFilesystem() throws Exception {
        NfsConfig sourceConfig = getNfsConfig(nfs);
        sourceConfig.setSubPath(sourceDirectoryName + NfsFile.separator + testFileName);

        FilesystemConfig targetConfig = new FilesystemConfig();
        targetConfig.setPath(filesystemTargetFile.getAbsolutePath());

        SyncConfig syncConfig = new SyncConfig().withSource(sourceConfig).withTarget(targetConfig);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);

        TestUtil.run(sync);

        byte[] sourceBytes = readBytes(sourceFile);
        byte[] targetBytes = Files.readAllBytes(filesystemTargetFile.toPath());
        Assertions.assertArrayEquals(sourceBytes, targetBytes);
    }

    /**
     * @param nfsFile
     * @return
     * @throws Exception
     */
    private byte[] readBytes(NfsFile nfsFile) throws Exception {
        NfsFileInputStream inputStream = null;
        try {
            inputStream = new NfsFileInputStream(nfsFile);
            byte[] bytes = new byte[inputStream.available()];
            inputStream.read(bytes);
            return bytes;
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    /**
     * @param nfs
     * @return
     */
    private NfsConfig getNfsConfig(Nfs nfs) {
        NfsConfig nfsConfig = new NfsConfig();
        nfsConfig.setServer(nfs.getServer());
        nfsConfig.setMountPath(nfs.getExportedPath() + testDirectoryPath);
        return nfsConfig;
    }
}

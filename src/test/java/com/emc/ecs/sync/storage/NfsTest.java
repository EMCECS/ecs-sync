/*
 * Copyright 2017 EMC Corporation. All Rights Reserved.
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

import com.emc.ecs.nfsclient.nfs.Nfs;
import com.emc.ecs.nfsclient.nfs.io.NfsFile;
import com.emc.ecs.nfsclient.nfs.io.NfsFileInputStream;
import com.emc.ecs.nfsclient.nfs.io.NfsFileOutputStream;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.emc.ecs.nfsclient.rpc.CredentialUnix;
import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
import com.emc.ecs.sync.config.storage.NfsConfig;
import com.emc.ecs.sync.util.RandomInputStream;
import com.emc.util.StreamUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.Properties;

public class NfsTest {
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

    @Before
    public void setup() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();
        String export = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_NFS_EXPORT);
        Assume.assumeNotNull(export);
        if (!export.contains(":")) throw new RuntimeException("invalid export: " + export);
        String server = export.split(":")[0];
        String mountPath = export.split(":")[1];

        nfs = new Nfs3(server, mountPath, new CredentialUnix(0, 0, null), 3);
        testDirectory = nfs.newFile(testDirectoryPath);
        testDirectory.mkdir();

        sourceDirectory = testDirectory.getChildFile(sourceDirectoryName);
        sourceDirectory.mkdir();
        sourceFile = sourceDirectory.getChildFile(testFileName);
        sourceFile.createNewFile();
        int size = 100 * 1024;
        NfsFileOutputStream outputStream = new NfsFileOutputStream(sourceFile);
        try {
            StreamUtil.copy(new RandomInputStream(size), outputStream, size);
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
        StreamUtil.copy(new RandomInputStream(size), new FileOutputStream(filesystemSourceFile), size);

        filesystemTargetDirectory = Files.createTempDirectory("ecs-sync-filesystem-target-test").toFile();
        if (!filesystemTargetDirectory.exists() || !filesystemTargetDirectory.isDirectory()) {
            throw new RuntimeException("unable to make filesystem target directory");
        }

        filesystemTargetFile = new File(filesystemTargetDirectory, testFileName);
    }

    @After
    public void teardown() throws Exception {
        deleteFile(targetFile);
        deleteFile(targetDirectory);
        deleteFile(sourceFile);
        deleteFile(sourceDirectory);
        deleteFile(testDirectory);
        for (File file : filesystemSourceDirectory.listFiles()) {
            file.delete();
        }
        filesystemSourceDirectory.delete();
        for (File file : filesystemTargetDirectory.listFiles()) {
            file.delete();
        }
        filesystemTargetDirectory.delete();
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
    public void testSingleFile() throws Exception {
        NfsConfig sourceConfig = getNfsConfig(nfs);
        sourceConfig.setSubPath(sourceDirectoryName + NfsFile.separator + testFileName);

        NfsConfig targetConfig = getNfsConfig(nfs);
        targetConfig.setSubPath(targetDirectoryName + NfsFile.separator + testFileName);

        SyncConfig syncConfig = new SyncConfig().withSource(sourceConfig).withTarget(targetConfig);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);

        sync.run();

        byte[] sourceBytes = readBytes(sourceFile);
        byte[] targetBytes = readBytes(targetFile);
        Assert.assertArrayEquals(sourceBytes, targetBytes);
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

        sync.run();

        byte[] sourceBytes = Files.readAllBytes(filesystemSourceFile.toPath());
        byte[] targetBytes = readBytes(targetFile);
        Assert.assertArrayEquals(sourceBytes, targetBytes);
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

        sync.run();

        byte[] sourceBytes = readBytes(sourceFile);
        byte[] targetBytes = Files.readAllBytes(filesystemTargetFile.toPath());
        Assert.assertArrayEquals(sourceBytes, targetBytes);
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

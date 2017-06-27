package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.filter.CuaExtractorConfig;
import com.emc.ecs.sync.config.filter.DxExtractorConfig;
import com.emc.ecs.sync.config.storage.CasConfig;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.CasStorageTest;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.storage.file.AbstractFilesystemStorage;
import com.emc.ecs.sync.test.TestConfig;
import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPPool;
import com.filepool.fplibrary.FPTag;
import org.apache.commons.compress.utils.Charsets;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.*;

public class CuaDxExtractionTest {
    private static final Logger log = LoggerFactory.getLogger(CasStorageTest.class);

    private String connectString;

    @Before
    public void setup() throws Exception {
        try {
            Properties syncProperties = TestConfig.getProperties();

            connectString = syncProperties.getProperty(TestConfig.PROP_CAS_CONNECT_STRING);

            Assume.assumeNotNull(connectString);
        } catch (FileNotFoundException e) {
            Assume.assumeFalse("Could not load ecs-sync.properties", true);
        }
    }

    @Test
    public void testCuaExtractorNfs() throws Exception {
        FPPool pool = new FPPool(connectString);
        byte[] data = "Hello CUA Exctractor!".getBytes(Charsets.UTF_8);

        List<String> clipIds = new ArrayList<>();
        clipIds.add(writeClip(pool, new byte[0], 0)); // empty file
        clipIds.add(writeClip(pool, data, 0)); // regular file

        // test 1 directory, 1 empty file, 1 regular file
        String nfsCsvData = "\"{directory}gateway_nfs\",\"gateway_nfs\",\"NFS\",\"508\",\"508\",\"-rwxrwxrwx\",\"1-May-2017 14:50:33\",\"1-May-2017 14:53:23\",\"25-Apr-2017 11:40:28\"\n" +
                "\"" + clipIds.get(0) + "\",\"gateway_nfs/empty-file\",\"NFS\",\"0\",\"0\",\"-rw-r--r--\",\"1-May-2017 14:50:33\",\"1-May-2017 14:50:33\",\"1-May-2017 14:51:35\"\n" +
                "\"" + clipIds.get(1) + "\",\"gateway_nfs/test\",\"NFS\",\"1001\",\"1002\",\"-rw-rw-r--\",\"1-May-2017 14:57:39\",\"1-May-2017 14:57:39\",\"1-May-2017 14:58:40\"\n" +
                "\"{symlink}gateway_nfs/test-link\",\"gateway_nfs/test-link\",\"NFS\",\"1001\",\"1002\",\"lrwxrwxrwx\",\"1-May-2017 14:57:39\",\"1-May-2017 14:57:39\",\"1-May-2017 14:58:40\",\"link-target\"";

        try {
            File listFile = File.createTempFile("cua-extract-test", "list");
            listFile.deleteOnExit();
            Files.write(listFile.toPath(), nfsCsvData.getBytes(Charsets.UTF_8));

            SyncConfig syncConfig = new SyncConfig().withSource(new CasConfig().withConnectionString(connectString))
                    .withTarget(new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false))
                    .withFilters(Collections.singletonList(new CuaExtractorConfig()));
            syncConfig.getOptions().withVerify(true).withSourceListFile(listFile.getPath());

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.run();

            Assert.assertEquals(0, sync.getStats().getObjectsFailed());
            Assert.assertEquals(4, sync.getStats().getObjectsComplete());

            TestStorage testStorage = (TestStorage) sync.getTarget();

            SyncObject object = testStorage.loadObject(testStorage.getIdentifier("gateway_nfs", true));
            Assert.assertTrue(object.getMetadata().isDirectory());
            // check times
            Assert.assertEquals(1493650233000L, object.getMetadata().getModificationTime().getTime());
            Assert.assertEquals(1493650403000L, object.getMetadata().getMetaChangeTime().getTime());
            // check ACL
            checkNfsAcl(object.getAcl(), "uid:508", "gid:508", 0b111111111);

            object = testStorage.loadObject(testStorage.getIdentifier("gateway_nfs/empty-file", false));
            Assert.assertFalse(object.getMetadata().isDirectory());
            // check data
            Assert.assertArrayEquals(new byte[0], ((TestStorage.TestSyncObject) object).getData());
            // check times
            Assert.assertEquals(1493650233000L, object.getMetadata().getModificationTime().getTime());
            Assert.assertEquals(1493650233000L, object.getMetadata().getMetaChangeTime().getTime());
            // check ACL
            checkNfsAcl(object.getAcl(), "uid:0", "gid:0", 0b110100100);

            object = testStorage.loadObject(testStorage.getIdentifier("gateway_nfs/test", false));
            Assert.assertFalse(object.getMetadata().isDirectory());
            // check data
            Assert.assertArrayEquals(data, ((TestStorage.TestSyncObject) object).getData());
            // check times
            Assert.assertEquals(1493650659000L, object.getMetadata().getModificationTime().getTime());
            Assert.assertEquals(1493650659000L, object.getMetadata().getMetaChangeTime().getTime());
            // check ACL
            checkNfsAcl(object.getAcl(), "uid:1001", "gid:1002", 0b110110100);

            object = testStorage.loadObject(testStorage.getIdentifier("gateway_nfs/test-link", false));
            Assert.assertFalse(object.getMetadata().isDirectory());
            // check times
            Assert.assertEquals(1493650659000L, object.getMetadata().getModificationTime().getTime());
            Assert.assertEquals(1493650659000L, object.getMetadata().getMetaChangeTime().getTime());
            // check ACL
            checkNfsAcl(object.getAcl(), "uid:1001", "gid:1002", 0b111111111);
            // check metadata
            Assert.assertEquals(AbstractFilesystemStorage.TYPE_LINK, object.getMetadata().getContentType());
            Assert.assertEquals("link-target", object.getMetadata().getUserMetadataValue(AbstractFilesystemStorage.META_LINK_TARGET));
        } finally {
            deleteClips(pool, clipIds);
            pool.Close();
        }
    }

    @Test
    public void testCuaExtractorCifs() throws Exception {
        FPPool pool = new FPPool(connectString);
        byte[] data = "Hello CUA CIFS Exctractor!".getBytes(Charsets.UTF_8);

        List<String> clipIds = new ArrayList<>();
        clipIds.add(writeClip(pool, new byte[0], 0)); // empty file
        clipIds.add(writeClip(pool, data, 0)); // regular file

        // test 1 directory, 1 empty file, 1 regular file
        String cifsCsvData = "\"{directory}cenlocal\",\"cenlocal\",\"<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>0</size></encoding>\",\"foo\",\"AQAEAABoAAAABAAAEAAAAAQAAAgAAACWGFvcxNLSAQUEAQAQAAAABAAACAAAABK2F4TF0tIBBQQCABAAAAAEAAAIAAAAErYXhMXS0gEFBAMAEAAAAAQAAAgAAAAStheExdLSAQUEBAAEAAAAEAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAAAAAAAAAAAAUEAQAQAAAABAAACAAAAAAAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAQUF\",\"AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAAQBAAAAAQAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAtAAHAAAAAAMYAP8BHwABAgAAAAAABSAAAAAgAgAAAAMUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAACxQAAAAAEAEBAAAAAAADAAAAAAADGACpABIAAQIAAAAAAAUgAAAAIQIAAAACGAAEAAAAAQIAAAAAAAUgAAAAIQIAAAACGAACAAAAAQIAAAAAAAUgAAAAIQIAAAU=\"\n" +
                "\"" + clipIds.get(0) + "\",\"cenlocal/test - Copy (2).txt\",\"<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>0</size></encoding>\",\"bar\",\"AQAEAABoAAAABAAAEAAAAAQAAAgAAAAiO/mKxdLSAQUEAQAQAAAABAAACAAAACI7+YrF0tIBBQQCABAAAAAEAAAIAAAA4hkKicXS0gEFBAMAEAAAAAQAAAgAAACeQQCJxtLSAQUEBAAEAAAAIAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAYAAAAAAAAAAUEAQAQAAAABAAACAAAABcAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAAUF\",\"AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAMAAAAC8AAAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAcAAEAAAAAAAYAP8BHwABAgAAAAAABSAAAAAgAgAAAAAUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAAABgAqQASAAECAAAAAAAFIAAAACECAAAF\"\n" +
                "\"" + clipIds.get(1) + "\",\"cenlocal/test - Copy (3).txt\",\"<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>26</size></encoding>\",\"\",\"AQAEAABoAAAABAAAEAAAAAQAAAgAAACajhqLxdLSAQUEAQAQAAAABAAACAAAAJqOGovF0tIBBQQCABAAAAAEAAAIAAAA4hkKicXS0gEFBAMAEAAAAAQAAAgAAAAuxwmJxtLSAQUEBAAEAAAAIAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAYAAAAAAAAAAUEAQAQAAAABAAACAAAABcAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAAUF\",\"AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAMAAAAC8AAAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAcAAEAAAAAAAYAP8BHwABAgAAAAAABSAAAAAgAgAAAAAUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAAABgAqQASAAECAAAAAAAFIAAAACECAAAF\"";

        try {
            File listFile = File.createTempFile("cua-extract-test", "list");
            listFile.deleteOnExit();
            Files.write(listFile.toPath(), cifsCsvData.getBytes(Charsets.UTF_8));

            SyncConfig syncConfig = new SyncConfig().withSource(new CasConfig().withConnectionString(connectString))
                    .withTarget(new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false))
                    .withFilters(Collections.singletonList(new CuaExtractorConfig()));
            syncConfig.getOptions().withVerify(true).withSourceListFile(listFile.getPath());

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.run();

            Assert.assertEquals(0, sync.getStats().getObjectsFailed());
            Assert.assertEquals(3, sync.getStats().getObjectsComplete());

            TestStorage testStorage = (TestStorage) sync.getTarget();

            SyncObject object = testStorage.loadObject(testStorage.getIdentifier("cenlocal", true));
            Assert.assertTrue(object.getMetadata().isDirectory());

            object = testStorage.loadObject(testStorage.getIdentifier("cenlocal/test - Copy (2).txt", false));
            Assert.assertFalse(object.getMetadata().isDirectory());
            // check data
            Assert.assertArrayEquals(new byte[0], ((TestStorage.TestSyncObject) object).getData());

            object = testStorage.loadObject(testStorage.getIdentifier("cenlocal/test - Copy (3).txt", false));
            Assert.assertFalse(object.getMetadata().isDirectory());
            // check data
            Assert.assertArrayEquals(data, ((TestStorage.TestSyncObject) object).getData());
        } finally {
            deleteClips(pool, clipIds);
            pool.Close();
        }
    }

    @Test
    public void testDxCasExtrator() throws Exception {
        FPPool pool = new FPPool(connectString);
        byte[] data = "Hello DX CIFS Exctractor!".getBytes(Charsets.UTF_8);

        List<String> clipIds = new ArrayList<>();
        clipIds.add(writeClip(pool, new byte[0], 1)); // empty file
        clipIds.add(writeClip(pool, data, 1)); // regular file

        // test 1 directory, 1 empty file, 1 regular file
        String dxCsvData = "\"{directory}cenlocal\",\"cenlocal\",\"<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>0</size></encoding>\",\"foo\",\"AQAEAABoAAAABAAAEAAAAAQAAAgAAACWGFvcxNLSAQUEAQAQAAAABAAACAAAABK2F4TF0tIBBQQCABAAAAAEAAAIAAAAErYXhMXS0gEFBAMAEAAAAAQAAAgAAAAStheExdLSAQUEBAAEAAAAEAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAAAAAAAAAAAAUEAQAQAAAABAAACAAAAAAAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAQUF\",\"AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAAQBAAAAAQAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAtAAHAAAAAAMYAP8BHwABAgAAAAAABSAAAAAgAgAAAAMUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAACxQAAAAAEAEBAAAAAAADAAAAAAADGACpABIAAQIAAAAAAAUgAAAAIQIAAAACGAAEAAAAAQIAAAAAAAUgAAAAIQIAAAACGAACAAAAAQIAAAAAAAUgAAAAIQIAAAU=\"\n" +
                "\"" + clipIds.get(0) + "\",\"cenlocal/test - Copy (2).txt\",\"<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>0</size></encoding>\",\"bar\",\"AQAEAABoAAAABAAAEAAAAAQAAAgAAAAiO/mKxdLSAQUEAQAQAAAABAAACAAAACI7+YrF0tIBBQQCABAAAAAEAAAIAAAA4hkKicXS0gEFBAMAEAAAAAQAAAgAAACeQQCJxtLSAQUEBAAEAAAAIAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAYAAAAAAAAAAUEAQAQAAAABAAACAAAABcAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAAUF\",\"AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAMAAAAC8AAAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAcAAEAAAAAAAYAP8BHwABAgAAAAAABSAAAAAgAgAAAAAUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAAABgAqQASAAECAAAAAAAFIAAAACECAAAF\"\n" +
                "\"" + clipIds.get(1) + "\",\"cenlocal/test - Copy (3).txt\",\"<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>26</size></encoding>\",\"\",\"AQAEAABoAAAABAAAEAAAAAQAAAgAAACajhqLxdLSAQUEAQAQAAAABAAACAAAAJqOGovF0tIBBQQCABAAAAAEAAAIAAAA4hkKicXS0gEFBAMAEAAAAAQAAAgAAAAuxwmJxtLSAQUEBAAEAAAAIAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAYAAAAAAAAAAUEAQAQAAAABAAACAAAABcAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAAUF\",\"AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAMAAAAC8AAAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAcAAEAAAAAAAYAP8BHwABAgAAAAAABSAAAAAgAgAAAAAUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAAABgAqQASAAECAAAAAAAFIAAAACECAAAF\"";

        try {
            File listFile = File.createTempFile("dx-extract-test", "list");
            listFile.deleteOnExit();
            Files.write(listFile.toPath(), dxCsvData.getBytes(Charsets.UTF_8));

            SyncConfig syncConfig = new SyncConfig().withSource(new CasConfig().withConnectionString(connectString))
                    .withTarget(new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false))
                    .withFilters(Collections.singletonList(new DxExtractorConfig()));
            syncConfig.getOptions().withVerify(true).withSourceListFile(listFile.getPath());

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.run();

            Assert.assertEquals(0, sync.getStats().getObjectsFailed());
            Assert.assertEquals(3, sync.getStats().getObjectsComplete());

            TestStorage testStorage = (TestStorage) sync.getTarget();

            SyncObject object = testStorage.loadObject(testStorage.getIdentifier("cenlocal", true));
            Assert.assertTrue(object.getMetadata().isDirectory());

            object = testStorage.loadObject(testStorage.getIdentifier("cenlocal/test - Copy (2).txt", false));
            Assert.assertFalse(object.getMetadata().isDirectory());
            // check data
            Assert.assertArrayEquals(new byte[0], ((TestStorage.TestSyncObject) object).getData());

            object = testStorage.loadObject(testStorage.getIdentifier("cenlocal/test - Copy (3).txt", false));
            Assert.assertFalse(object.getMetadata().isDirectory());
            // check data
            Assert.assertArrayEquals(data, ((TestStorage.TestSyncObject) object).getData());
        } finally {
            deleteClips(pool, clipIds);
            pool.Close();
        }
    }

    @Test
    public void testDxUniversalExtractor() throws Exception {
        SyncOptions syncOptions = new SyncOptions();

        com.emc.ecs.sync.config.storage.TestConfig testConfig = new com.emc.ecs.sync.config.storage.TestConfig();
        testConfig.setDiscardData(false);

        TestStorage testStorage = new TestStorage();
        testStorage.withConfig(testConfig).withOptions(syncOptions);

        // create 3 source objects
        byte[] data = "Hello DX CIFS Exctractor!".getBytes(Charsets.UTF_8);
        testStorage.createObject(new SyncObject(testStorage, "cenlocal",
                new ObjectMetadata().withDirectory(true), new ByteArrayInputStream(new byte[0]), new ObjectAcl()));
        testStorage.createObject(new SyncObject(testStorage, "cenlocal/test - Copy (2).txt",
                new ObjectMetadata().withContentLength(0), new ByteArrayInputStream(new byte[0]), new ObjectAcl()));
        testStorage.createObject(new SyncObject(testStorage, "cenlocal/test - Copy (3).txt",
                new ObjectMetadata().withContentLength(data.length), new ByteArrayInputStream(data), new ObjectAcl()));

        String dxCsvData = "\"/root/cenlocal\",\"cenlocal\",\"<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>0</size></encoding>\",\"foo\",\"AQAEAABoAAAABAAAEAAAAAQAAAgAAACWGFvcxNLSAQUEAQAQAAAABAAACAAAABK2F4TF0tIBBQQCABAAAAAEAAAIAAAAErYXhMXS0gEFBAMAEAAAAAQAAAgAAAAStheExdLSAQUEBAAEAAAAEAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAAAAAAAAAAAAUEAQAQAAAABAAACAAAAAAAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAQUF\",\"AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAAQBAAAAAQAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAtAAHAAAAAAMYAP8BHwABAgAAAAAABSAAAAAgAgAAAAMUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAACxQAAAAAEAEBAAAAAAADAAAAAAADGACpABIAAQIAAAAAAAUgAAAAIQIAAAACGAAEAAAAAQIAAAAAAAUgAAAAIQIAAAACGAACAAAAAQIAAAAAAAUgAAAAIQIAAAU=\"\n" +
                "\"/root/cenlocal/test - Copy (2).txt\",\"cenlocal/test - Copy (2).txt\",\"<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>0</size></encoding>\",\"bar\",\"AQAEAABoAAAABAAAEAAAAAQAAAgAAAAiO/mKxdLSAQUEAQAQAAAABAAACAAAACI7+YrF0tIBBQQCABAAAAAEAAAIAAAA4hkKicXS0gEFBAMAEAAAAAQAAAgAAACeQQCJxtLSAQUEBAAEAAAAIAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAYAAAAAAAAAAUEAQAQAAAABAAACAAAABcAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAAUF\",\"AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAMAAAAC8AAAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAcAAEAAAAAAAYAP8BHwABAgAAAAAABSAAAAAgAgAAAAAUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAAABgAqQASAAECAAAAAAAFIAAAACECAAAF\"\n" +
                "\"/root/cenlocal/test - Copy (3).txt\",\"cenlocal/test - Copy (3).txt\",\"<encoding><compression>none</compression><encryption><keyname>default</keyname><cryptotype>none</cryptotype></encryption><size>26</size></encoding>\",\"\",\"AQAEAABoAAAABAAAEAAAAAQAAAgAAACajhqLxdLSAQUEAQAQAAAABAAACAAAAJqOGovF0tIBBQQCABAAAAAEAAAIAAAA4hkKicXS0gEFBAMAEAAAAAQAAAgAAAAuxwmJxtLSAQUEBAAEAAAAIAAAAAUEAQBKAAAABAAAEAAAAAQAAAgAAAAYAAAAAAAAAAUEAQAQAAAABAAACAAAABcAAAAAAAAABQQCAAQAAAABAAAABAMAAQAAAAAEBAABAAAAAAUF\",\"AQAEAAAWAAAAAhEHAAAAQwBFAE4AVABFAFIAQQADEQQBACAAAAAcAAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRXAQAAAQCAMAAAAC8AAAAAQAEgBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACaD4Pm3pfpsj/6bJFUBAAAAQUAAAAAAAUVAAAAmg+D5t6X6bI/+myRAQIAAAIAcAAEAAAAAAAYAP8BHwABAgAAAAAABSAAAAAgAgAAAAAUAP8BHwABAQAAAAAABRIAAAAAACQA/wEfAAEFAAAAAAAFFQAAAJoPg+bel+myP/pskVQEAAAAABgAqQASAAECAAAAAAAFIAAAACECAAAF\"";

        File listFile = File.createTempFile("dx-extract-test", "list");
        listFile.deleteOnExit();
        Files.write(listFile.toPath(), dxCsvData.getBytes(Charsets.UTF_8));

        SyncConfig syncConfig = new SyncConfig().withOptions(syncOptions).withTarget(new com.emc.ecs.sync.config.storage.TestConfig().withDiscardData(false))
                .withFilters(Collections.singletonList(new DxExtractorConfig()));
        syncConfig.getOptions().withRecursive(false).withVerify(true).withSourceListFile(listFile.getPath());

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(testStorage);
        sync.run();

        Assert.assertEquals(0, sync.getStats().getObjectsFailed());
        Assert.assertEquals(3, sync.getStats().getObjectsComplete());

        TestStorage targetStorage = (TestStorage) sync.getTarget();

        SyncObject object = targetStorage.loadObject(targetStorage.getIdentifier("cenlocal", true));
        Assert.assertTrue(object.getMetadata().isDirectory());

        object = targetStorage.loadObject(targetStorage.getIdentifier("cenlocal/test - Copy (2).txt", false));
        Assert.assertFalse(object.getMetadata().isDirectory());
        // check data
        Assert.assertArrayEquals(new byte[0], ((TestStorage.TestSyncObject) object).getData());

        object = targetStorage.loadObject(targetStorage.getIdentifier("cenlocal/test - Copy (3).txt", false));
        Assert.assertFalse(object.getMetadata().isDirectory());
        // check data
        Assert.assertArrayEquals(data, ((TestStorage.TestSyncObject) object).getData());
    }

    private void checkNfsAcl(ObjectAcl acl, String user, String group, int mode) {
        // find group owner in ACL
        Iterator<String> groupI = acl.getGroupGrants().keySet().iterator();
        String firstGroup = null;
        if (groupI.hasNext()) firstGroup = groupI.next();
        if (AbstractFilesystemStorage.OTHER_GROUP.equals(firstGroup) && groupI.hasNext()) firstGroup = groupI.next();

        Assert.assertEquals(user, acl.getOwner());
        Assert.assertEquals(group, firstGroup);
        Set<String> userPerms = acl.getUserGrants().get(user);
        Assert.assertNotNull(userPerms);
        Assert.assertEquals((mode & 0b0100000000) != 0, userPerms.contains(AbstractFilesystemStorage.READ));
        Assert.assertEquals((mode & 0b0010000000) != 0, userPerms.contains(AbstractFilesystemStorage.WRITE));
        Assert.assertEquals((mode & 0b0001000000) != 0, userPerms.contains(AbstractFilesystemStorage.EXECUTE));
        Set<String> groupPerms = acl.getGroupGrants().get(group);
        Assert.assertNotNull(groupPerms);
        Assert.assertEquals((mode & 0b0000100000) != 0, groupPerms.contains(AbstractFilesystemStorage.READ));
        Assert.assertEquals((mode & 0b0000010000) != 0, groupPerms.contains(AbstractFilesystemStorage.WRITE));
        Assert.assertEquals((mode & 0b0000001000) != 0, groupPerms.contains(AbstractFilesystemStorage.EXECUTE));
        Set<String> otherPerms = acl.getGroupGrants().get(AbstractFilesystemStorage.OTHER_GROUP);
        Assert.assertNotNull(otherPerms);
        Assert.assertEquals((mode & 0b0000000100) != 0, otherPerms.contains(AbstractFilesystemStorage.READ));
        Assert.assertEquals((mode & 0b0000000010) != 0, otherPerms.contains(AbstractFilesystemStorage.WRITE));
        Assert.assertEquals((mode & 0b0000000001) != 0, otherPerms.contains(AbstractFilesystemStorage.EXECUTE));
    }

    private String writeClip(FPPool pool, byte[] data, int blobIndex) throws Exception {
        FPClip clip = new FPClip(pool);
        FPTag topTag = clip.getTopTag();

        for (int i = 0; i < blobIndex; i++) {
            FPTag tag = new FPTag(topTag, "md_tag");
            tag.Close();
        }

        FPTag tag = new FPTag(topTag, "data_tag");
        tag.BlobWrite(new ByteArrayInputStream(data));
        tag.Close();

        topTag.Close();

        String clipId = clip.Write();
        clip.Close();

        return clipId;
    }

    private void deleteClips(FPPool pool, List<String> clipIds) throws Exception {
        for (String clipId : clipIds) {
            try {
                FPClip.Delete(pool, clipId);
            } catch (Exception e) {
                log.warn("could not delete clip " + clipId, e);
            }
        }
    }
}

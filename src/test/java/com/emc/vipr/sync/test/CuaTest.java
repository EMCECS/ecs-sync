/*
 * Copyright 2014 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.test;

import com.emc.vipr.sync.ViPRSync;
import com.emc.vipr.sync.source.CasSource;
import com.emc.vipr.sync.target.CuaFilesystemTarget;
import com.emc.vipr.sync.test.util.SyncConfig;
import com.emc.vipr.sync.util.CasInputStream;
import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPPool;
import com.filepool.fplibrary.FPTag;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CuaTest {
    protected String connectString;

    @Before
    public void setup() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();

        connectString = syncProperties.getProperty(SyncConfig.PROP_CAS_CONNECT_STRING + "2");
        Assume.assumeNotNull(connectString);
    }

    @Test
    public void testCuaToFilesystem() throws Exception {
        Logger.getLogger(ViPRSync.class).setLevel(Level.DEBUG);

        FPPool pool = new FPPool(connectString);
        List<String> clipIds = createTestClips(pool, 102400, 300);

        CasSource source = new CasSource();
        source.setConnectionString(connectString);

        File tmpFile = File.createTempFile("test", "test");
        tmpFile.deleteOnExit();
        File dir = tmpFile.getParentFile();

        System.out.println("Writing to " + dir);

        CuaFilesystemTarget target = new CuaFilesystemTarget();
        target.setTarget(dir.getAbsolutePath());

        ViPRSync sync = new ViPRSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setTimingsEnabled(true);
        sync.setSyncThreadCount(10);

        sync.run();

        System.out.println(sync.getStatsString());

        delete(pool, clipIds);
    }

    protected List<String> createTestClips(FPPool pool, int maxBlobSize, int thisMany) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(CAS_SETUP_THREADS);

        System.out.print("Creating clips");

        List<String> clipIds = Collections.synchronizedList(new ArrayList<String>());
        for (int clipIdx = 0; clipIdx < thisMany; clipIdx++) {
            service.submit(new ClipWriter(pool, clipIds, maxBlobSize));
        }

        service.shutdown();
        service.awaitTermination(CAS_SETUP_WAIT_MINUTES, TimeUnit.MINUTES);
        service.shutdownNow();

        System.out.println();

        return clipIds;
    }

    protected void delete(FPPool pool, List<String> clipIds) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(CAS_SETUP_THREADS);

        System.out.print("Deleting clips");

        for (String clipId : clipIds) {
            service.submit(new ClipDeleter(pool, clipId));
        }

        service.shutdown();
        service.awaitTermination(CAS_SETUP_WAIT_MINUTES, TimeUnit.MINUTES);
        service.shutdownNow();

        System.out.println();
    }

    static class ClipWriter implements Runnable {
        private static int counter = 0;

        private FPPool pool;
        private List<String> clipIds;
        private int maxBlobSize;
        private Random random;

        public ClipWriter(FPPool pool, List<String> clipIds, int maxBlobSize) {
            this.pool = pool;
            this.clipIds = clipIds;
            this.maxBlobSize = maxBlobSize;
            random = new Random();
        }

        @Override
        public void run() {
            try {
                FPClip clip = new FPClip(pool, CLIP_NAME);
                FPTag topTag = clip.getTopTag();

                FPTag tag = new FPTag(topTag, BLOB_TAG_NAME);
                // random blob length (<= maxBlobSize)
                byte[] blobContent = new byte[random.nextInt(maxBlobSize) + 1];
                // random blob content
                random.nextBytes(blobContent);
                tag.BlobWrite(new CasInputStream(new ByteArrayInputStream(blobContent), blobContent.length));
                tag.Close();

                int thisNum = ++counter;
                long now = System.currentTimeMillis();
                tag = new FPTag(topTag, ATTRIBUTE_TAG_NAME);
                tag.setAttribute(PATH_ATTRIBUTE, "/foo/bar" + (thisNum / 10 + 1) + "/file_" + (thisNum % 10) + ".blob");
                tag.setAttribute(UID_ATTRIBUTE, 1100 + (counter / 100 + 1));
                tag.setAttribute(GID_ATTRIBUTE, 1100 + (counter / 100 + 1));
                tag.setAttribute(ITIME_ATTRIBUTE, now);
                tag.setAttribute(MTIME_ATTRIBUTE, now);
                tag.setAttribute(ATIME_ATTRIBUTE, now);
                tag.setAttribute(SIZE_HI_ATTRIBUTE, 0);
                tag.setAttribute(SIZE_LO_ATTRIBUTE, blobContent.length);
                tag.Close();

                topTag.Close();
                clipIds.add(clip.Write());
                clip.Close();
                System.out.print(".");
            } catch (Exception e) {
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
        }
    }

    class ClipDeleter implements Runnable {
        private FPPool pool;
        private String clipId;

        public ClipDeleter(FPPool pool, String clipId) {
            this.pool = pool;
            this.clipId = clipId;
        }

        @Override
        public void run() {
            try {
                System.out.print(".");
                FPClip.Delete(pool, clipId);
            } catch (Exception e) {
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
        }
    }

    private static final String CLIP_NAME = "Storigen_File_Gateway";
    private static final String ATTRIBUTE_TAG_NAME = "Storigen_File_Gateway_File_Attributes";
    private static final String BLOB_TAG_NAME = "Storigen_File_Gateway_Blob";

    private static final String PATH_ATTRIBUTE = "Storigen_File_Gateway_File_Path0";
    private static final String UID_ATTRIBUTE = "Storigen_File_Gateway_File_Owner";
    private static final String GID_ATTRIBUTE = "Storigen_File_Gateway_File_Group";
    private static final String ITIME_ATTRIBUTE = "Storigen_File_Gateway_File_CTime"; // Windows ctime means create time
    private static final String MTIME_ATTRIBUTE = "Storigen_File_Gateway_File_MTime";
    private static final String ATIME_ATTRIBUTE = "Storigen_File_Gateway_File_ATime";
    private static final String SIZE_HI_ATTRIBUTE = "Storigen_File_Gateway_File_Size_Hi";
    private static final String SIZE_LO_ATTRIBUTE = "Storigen_File_Gateway_File_Size_Lo";

    protected static final int CAS_SETUP_THREADS = 20;
    protected static final int CAS_SETUP_WAIT_MINUTES = 10;
}

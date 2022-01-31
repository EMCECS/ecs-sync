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
import com.emc.ecs.sync.config.storage.CasConfig;
import com.emc.ecs.sync.storage.cas.*;
import com.emc.ecs.sync.test.ByteAlteringFilter;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.util.Iso8601Util;
import com.emc.ecs.sync.util.LoggingUtil;
import com.emc.ecs.sync.util.RandomInputStream;
import com.filepool.fplibrary.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CasStorageTest {
    private static final Logger log = LoggerFactory.getLogger(CasStorageTest.class);

    private static final int CLIP_OPTIONS = 0;
    private static final int BUFFER_SIZE = 1048576; // 1MB
    private static final int CAS_THREADS = 32;
    private static final int CAS_SETUP_WAIT_MINUTES = 5;

    private String connectString1, connectString2, centeraConnectString;
    private Level logLevel;
    private byte[] duplicateBlobData;

    @BeforeEach
    public void setup() throws Exception {
        logLevel = LoggingUtil.getRootLogLevel();
        // TODO: handle log elevation in a different way
        LoggingUtil.setRootLogLevel(Level.INFO);
        try {
            Properties syncProperties = TestConfig.getProperties();

            connectString1 = syncProperties.getProperty(TestConfig.PROP_CAS_CONNECT_STRING);
            connectString2 = syncProperties.getProperty(TestConfig.PROP_CAS_CONNECT_STRING + "2");
            centeraConnectString = syncProperties.getProperty(TestConfig.PROP_CENTERA_CONNECT_STRING);

            Assumptions.assumeTrue(connectString1 != null && connectString2 != null);
        } catch (FileNotFoundException e) {
            Assumptions.assumeFalse(true, "Could not load ecs-sync.properties");
        }

        duplicateBlobData = new byte[13 * 1024];
        new Random().nextBytes(this.duplicateBlobData);
    }

    public void tearDown() throws Exception {
        LoggingUtil.setRootLogLevel(logLevel);
    }

    @Test
    public void testPipedStreams() throws Exception {
        Random random = new Random();

        // test smaller than pipe buffer
        byte[] source = new byte[random.nextInt(BUFFER_SIZE) + 1];
        random.nextBytes(source);
        String md5 = DatatypeConverter.printHexBinary(MessageDigest.getInstance("MD5").digest(source));
        Assertions.assertEquals(md5, pipeAndGetMd5(source), "MD5 mismatch");

        // test larger than pipe buffer
        source = new byte[random.nextInt(BUFFER_SIZE) + BUFFER_SIZE + 1];
        random.nextBytes(source);
        md5 = DatatypeConverter.printHexBinary(MessageDigest.getInstance("MD5").digest(source));
        Assertions.assertEquals(md5, pipeAndGetMd5(source), "MD5 mismatch");
    }

    private String pipeAndGetMd5(byte[] source) throws Exception {
        PipedInputStream pin = new PipedInputStream(BUFFER_SIZE);
        PipedOutputStream pout = new PipedOutputStream(pin);

        Producer producer = new Producer(source, pout);

        // produce in parallel
        Thread producerThread = new Thread(producer);
        producerThread.start();

        // consume inside this thread
        byte[] dest = new byte[source.length];
        try {
            int read = 0;
            while (read < dest.length && read != -1) {
                read += pin.read(dest, read, dest.length - read);
            }
        } finally {
            try {
                pin.close();
            } catch (Throwable t) {
                // ignore
            }
        }

        // synchronize
        producerThread.join();

        return DatatypeConverter.printHexBinary(MessageDigest.getInstance("MD5").digest(dest));
    }

    @Test
    public void testCasSingleObject() throws Exception {
        FPPool sourcePool = new FPPool(connectString1);
        FPPool targetPool = new FPPool(connectString2);

        String clipID = null;
        try {
            // create clip in source (<=1MB blob size) - capture summary for comparison
            StringWriter sourceSummary = new StringWriter();
            List<String> clipIds = createTestClips(sourcePool, 1048576, 1, sourceSummary);
            clipID = clipIds.iterator().next();

            // open clip in source
            FPClip clip = new FPClip(sourcePool, clipID, FPLibraryConstants.FP_OPEN_FLAT);

            // buffer CDF
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            clip.RawRead(baos);

            // write CDF to target
            FPClip targetClip = new FPClip(targetPool, clipID, new ByteArrayInputStream(baos.toByteArray()), CLIP_OPTIONS);

            // migrate blobs
            FPTag tag, targetTag;
            int tagCount = 0;
            while ((tag = clip.FetchNext()) != null) {
                targetTag = targetClip.FetchNext();
                Assertions.assertEquals(tag.getTagName(), targetTag.getTagName(), "Tag names don't match");
                Assertions.assertTrue(Arrays.equals(tag.getAttributes(), targetTag.getAttributes()),
                        "Tag " + tag.getTagName() + " attributes not equal");

                int blobStatus = tag.BlobExists();
                if (blobStatus == 1) {
                    PipedInputStream pin = new PipedInputStream(BUFFER_SIZE);
                    PipedOutputStream pout = new PipedOutputStream(pin);
                    BlobReader reader = new BlobReader(tag, pout);

                    // start reading in parallel
                    Thread readThread = new Thread(reader);
                    readThread.start();

                    // write inside this thread
                    targetTag.BlobWrite(pin);

                    readThread.join(); // this shouldn't do anything, but just in case

                    if (!reader.isSuccess()) throw new Exception("blob read failed", reader.getError());
                } else {
                    if (blobStatus != -1)
                        System.out.println("blob unavailable, clipId=" + clipID + ", tagNum=" + tagCount + ", blobStatus=" + blobStatus);
                }
                tag.Close();
                targetTag.Close();
                tagCount++;
            }

            clip.Close();

            Assertions.assertEquals(clipID, targetClip.Write(), "clip IDs not equal");
            targetClip.Close();

            // check target blob data
            targetClip = new FPClip(targetPool, clipID, FPLibraryConstants.FP_OPEN_FLAT);
            Assertions.assertEquals(sourceSummary.toString(), summarizeClip(targetClip), "content mismatch");
            targetClip.Close();
        } finally {
            // delete in source and target
            if (clipID != null) {
                try {
                    FPClip.Delete(sourcePool, clipID);
                    FPClip.Delete(targetPool, clipID);
                } catch (Throwable t) {
                    log.warn("could not delete clip", t);
                }
            }
            try {
                sourcePool.Close();
            } catch (Throwable t) {
                log.warn("failed to close source pool", t);
            }
            try {
                targetPool.Close();
            } catch (Throwable t) {
                log.warn("failed to close dest pool", t);
            }
        }
    }

    @Test
    public void testReopenClip() throws Exception {
        FPPool pool = new FPPool(connectString1);
        String clipID = null;

        try {
            // create clip in source (<=1MB blob size) - capture summary for comparison
            StringWriter sourceSummary = new StringWriter();
            List<String> clipIds = createTestClips(pool, 1048576, 1, sourceSummary);
            clipID = clipIds.iterator().next();

            // open clip
            FPClip clip = new FPClip(pool, clipID);
            long size = clip.getTotalSize();
            log.info("clip {} has total size {} bytes", clipID, size);
            // close clip
            clip.Close();

            // reopen clip
            clip = new FPClip(pool, clipID, FPLibraryConstants.FP_OPEN_FLAT);
            Assertions.assertNotNull(clip);
            clip.Close();
        } finally {
            if (clipID != null) FPClip.Delete(pool, clipID);
            try {
                pool.Close();
            } catch (Throwable t) {
                log.warn("failed to close pool", t);
            }
        }
    }

    @Test
    public void testBlobWriteEmpty() throws Exception {
        Assumptions.assumeTrue(connectString1 != null);

        FPPool pool = new FPPool(connectString1);

        String clipID = null;
        try {
            clipID = createTestClips(pool, 0, 1, null).iterator().next();
        } finally {
            if (clipID != null) FPClip.Delete(pool, clipID);
            try {
                pool.Close();
            } catch (Throwable t) {
                log.warn("failed to close pool", t);
            }
        }
    }

    // this simulates a long wait for Centera followed by a read error (which manifests as a zero-byte stream for the
    // write due to the piped stream)
    // the goal is to see if this causes a crash in the CAS SDk
    @Test
    public void testBlobWriteDelayed() throws Exception {
        Assumptions.assumeTrue(connectString1 != null);

        FPPool pool = new FPPool(connectString1);

        String clipID = null;
        try {
            FPClip clip = new FPClip(pool);
            FPTag topTag = clip.getTopTag();

            FPTag tag = new FPTag(topTag, "test_delay_tag");
            // make the stream wait 20 seconds before closing with no data
            tag.BlobWrite(new DelayedInputStream(new ByteArrayInputStream(new byte[0]), 20000));
            tag.Close();

            topTag.Close();
            clipID = clip.Write();
            clip.Close();
        } finally {
            if (clipID != null) FPClip.Delete(pool, clipID);
            try {
                pool.Close();
            } catch (Throwable t) {
                log.warn("failed to close pool", t);
            }
        }
    }

    // this simulates an ECS write error while the reader thread is blocked due to a full piped stream
    // the goal is to see if this causes a crash in the CAS SDK
    @Test
    public void testBlobReadCloseEarly() throws Exception {
        int blobSize = 1024000, bufferSize = 102400;
        Assumptions.assumeTrue(connectString2 != null);

        CasPool pool = new CasPool(connectString2);

        String clipID = null;
        CasClip casClip = null;
        CasTag casTag = null;
        try {
            // first, write a 1MB clip
            FPClip clip = new FPClip(pool);
            FPTag topTag = clip.getTopTag();

            FPTag tag = new FPTag(topTag, "test_blocked_read_exception");

            tag.BlobWrite(new RandomInputStream(blobSize));
            tag.Close();

            topTag.Close();
            clipID = clip.Write();
            clip.Close();

            // open clip/blob for reading
            casClip = new CasClip(pool, clipID, FPLibraryConstants.FP_OPEN_FLAT);
            casTag = casClip.FetchNext();
            Assertions.assertEquals(1, casTag.BlobExists());

            // create a piped stream with 100k buffer and a reader thread
            // (BlobInputStream does all that)
            BlobInputStream bis = new BlobInputStream(casTag, bufferSize);

            // wait for the read buffer to fill
            int waitInterval = 500, waited = 0, maxWait = 10000; // don't wait longer than 10 seconds
            while (bis.available() < bufferSize && waited < maxWait) {
                Thread.sleep(waitInterval);
                waited += waitInterval;
            }

            // at this point reader thread is blocked, now simulate a write error, which would simply close the stream
            // early
            bis.close();

        } finally {
            if (casTag != null) try {
                casTag.close();
            } catch (Throwable t) {
                log.warn("could not close tag", t);
            }
            if (casClip != null) try {
                casClip.close();
            } catch (Throwable t) {
                log.warn("could not close clip " + casClip.getClipID(), t);
            }
            if (clipID != null) FPClip.Delete(pool, clipID);
            try {
                pool.Close();
            } catch (Throwable t) {
                log.warn("failed to close pool", t);
            }
        }
    }

    @Test
    public void testBlobExistsRaw() throws Exception {
        Assumptions.assumeTrue(centeraConnectString != null);

        byte[] blobData = new byte[13 * 1024];
        new Random().nextBytes(blobData);

        FPPool pool1 = new FPPool(centeraConnectString);
        FPPool pool2 = new FPPool(connectString2);

        Assertions.assertTrue(pool1.getCapability(FPLibraryConstants.FP_PRIVILEGEDDELETE, FPLibraryConstants.FP_ALLOWED).equals("True"), "priv-delete not supported in Centera pool");

        try {
            FPClip clip = new FPClip(pool1);
            FPTag topTag = clip.getTopTag();

            FPTag tag = new FPTag(topTag, "blob1");
            tag.BlobWrite(new ByteArrayInputStream(blobData));
            tag.Close();

            tag = new FPTag(topTag, "blob1");
            tag.BlobWrite(new ByteArrayInputStream(blobData), FPLibraryConstants.FP_OPTION_CLIENT_CALCID);
            tag.Close();

            topTag.Close();
            String clipId = clip.Write();
            clip.Close();

            // open clip in source
            clip = new FPClip(pool1, clipId, FPLibraryConstants.FP_OPEN_FLAT); // raw read

            // buffer CDF
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            clip.RawRead(baos);

            // write CDF to target
            FPClip targetClip = new FPClip(pool2, clipId, new ByteArrayInputStream(baos.toByteArray()), CLIP_OPTIONS);

            // migrate blob1
            tag = clip.FetchNext();
            FPTag targetTag = targetClip.FetchNext();

            Assertions.assertEquals(1, tag.BlobExists());
            Assertions.assertEquals(0, targetTag.BlobExists());

            PipedInputStream pin = new PipedInputStream(BUFFER_SIZE);
            PipedOutputStream pout = new PipedOutputStream(pin);
            BlobReader reader = new BlobReader(tag, pout);

            // start reading in parallel
            Thread readThread = new Thread(reader);
            readThread.start();

            // write inside this thread
            targetTag.BlobWrite(pin);

            readThread.join(); // this shouldn't do anything, but just in case

            if (!reader.isSuccess()) throw new Exception("blob read failed", reader.getError());

            tag.Close();
            targetTag.Close();

            // migrate blob2 (should already exist)
            tag = clip.FetchNext();
            targetTag = targetClip.FetchNext();

            Assertions.assertEquals(1, tag.BlobExists());
            Assertions.assertEquals(1, targetTag.BlobExists());

            tag.Close();
            targetTag.Close();

            clip.Close();

            Assertions.assertEquals(clipId, targetClip.Write(), "clip IDs not equal");
            targetClip.Close();

            // check target blob data
            clip = new FPClip(pool1, clipId, FPLibraryConstants.FP_OPEN_FLAT);
            targetClip = new FPClip(pool2, clipId, FPLibraryConstants.FP_OPEN_FLAT);
            Assertions.assertEquals(summarizeClip(clip), summarizeClip(targetClip), "content mismatch");
            clip.Close();
            targetClip.Close();

            // delete in source and target
            FPClip.AuditedDelete(pool1, clipId, "ecs-sync CasStorageTest clip", FPLibraryConstants.FP_OPTION_DELETE_PRIVILEGED);
            FPClip.Delete(pool2, clipId);
        } finally {
            try {
                pool1.Close();
            } catch (Throwable t) {
                log.warn("failed to close source pool", t);
            }
            try {
                pool2.Close();
            } catch (Throwable t) {
                log.warn("failed to close dest pool", t);
            }
        }
    }

    @Test
    public void testSyncSingleClip() throws Exception {
        testSyncClipList(1, 102400);
    }

    @Test
    public void testSyncClipListSmallBlobs() throws Exception {
        int numClips = 250, maxBlobSize = 102400;
        testSyncClipList(numClips, maxBlobSize);
    }

    @Test
    public void testSyncClipListLargeBlobs() throws Exception {
        int numClips = 25, maxBlobSize = 2048000;
        testSyncClipList(numClips, maxBlobSize);
    }

    private void testSyncClipList(int numClips, int maxBlobSize) throws Exception {
        FPPool sourcePool = new FPPool(connectString1);
        FPPool destPool = new FPPool(connectString2);

        // create random data (capture summary for comparison)
        StringWriter sourceSummary = new StringWriter();
        List<String> clipIds = createTestClips(sourcePool, maxBlobSize, numClips, sourceSummary);

        try {
            // write clip file
            File clipFile = File.createTempFile("clip", "lst");
            clipFile.deleteOnExit();
            BufferedWriter writer = new BufferedWriter(new FileWriter(clipFile));
            for (String clipId : clipIds) {
                log.debug("created {}", clipId);
                writer.write(clipId);
                writer.newLine();
            }
            writer.close();

            EcsSync sync = createEcsSync(connectString1, connectString2, CAS_THREADS, true);
            sync.getSyncConfig().getOptions().setSourceListFile(clipFile.getAbsolutePath());

            run(sync);

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(numClips, sync.getStats().getObjectsComplete());

            String destSummary = summarize(destPool, clipIds);
            Assertions.assertEquals(sourceSummary.toString(), destSummary, "query summaries different");
        } finally {
            delete(sourcePool, clipIds);
            delete(destPool, clipIds);
            try {
                sourcePool.Close();
            } catch (Throwable t) {
                log.warn("failed to close source pool", t);
            }
            try {
                destPool.Close();
            } catch (Throwable t) {
                log.warn("failed to close dest pool", t);
            }
        }
    }

    @Test
    public void testClipListDuplicateBlobs() throws Exception {
        Assumptions.assumeTrue(centeraConnectString != null);

        FPPool sourcePool = new FPPool(centeraConnectString);
        FPPool destPool = new FPPool(connectString2);

        Assertions.assertTrue(sourcePool.getCapability(FPLibraryConstants.FP_PRIVILEGEDDELETE, FPLibraryConstants.FP_ALLOWED).equals("True"), "priv-delete not supported in centera pool");
        Assertions.assertTrue(destPool.getCapability(FPLibraryConstants.FP_PRIVILEGEDDELETE, FPLibraryConstants.FP_ALLOWED).equals("True"), "priv-delete not supported in target pool");

        // create random data (capture summary for comparison)
        int clipCount = 100;
        StringWriter sourceSummary = new StringWriter();
        AtomicInteger duplicateCount = new AtomicInteger();
        List<String> clipIds = createTestClips(sourcePool, 51200, clipCount, sourceSummary, 50, duplicateCount);

        String dupClipId = null;
        try {
            Assertions.assertTrue(duplicateCount.get() > 10); // we need at least this many for a valid test

            // write duplicated blob and raw-write to target to ensure we don't write it simultaneously during sync and mess up the count
            FPClip clip = new FPClip(sourcePool);
            FPTag topTag = clip.getTopTag();
            FPTag tag = new FPTag(topTag, "blob1");
            tag.BlobWrite(new ByteArrayInputStream(duplicateBlobData));
            tag.Close();
            topTag.Close();
            dupClipId = clip.Write();
            clip.Close();
            clip = new FPClip(sourcePool, dupClipId, FPLibraryConstants.FP_OPEN_FLAT); // raw read
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            clip.RawRead(baos);
            FPClip targetClip = new FPClip(destPool, dupClipId, new ByteArrayInputStream(baos.toByteArray()), CLIP_OPTIONS);
            tag = clip.FetchNext();
            FPTag targetTag = targetClip.FetchNext();
            PipedInputStream pin = new PipedInputStream(BUFFER_SIZE);
            PipedOutputStream pout = new PipedOutputStream(pin);
            BlobReader reader = new BlobReader(tag, pout);
            Thread readThread = new Thread(reader);
            readThread.start();
            targetTag.BlobWrite(pin);
            readThread.join(); // this shouldn't do anything, but just in case
            if (!reader.isSuccess()) throw new Exception("blob read failed", reader.getError());
            tag.Close();
            targetTag.Close();
            clip.Close();
            targetClip.Close();

            // write clip file
            File clipFile = File.createTempFile("clip", "lst");
            clipFile.deleteOnExit();
            BufferedWriter writer = new BufferedWriter(new FileWriter(clipFile));
            for (String clipId : clipIds) {
                log.debug("created {}", clipId);
                writer.write(clipId);
                writer.newLine();
            }
            writer.close();

            EcsSync sync = createEcsSync(centeraConnectString, connectString2, CAS_THREADS, true);
            sync.getSyncConfig().getOptions().setSourceListFile(clipFile.getAbsolutePath());

            run(sync);

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(clipCount, sync.getStats().getObjectsComplete());

            String destSummary = summarize(destPool, clipIds);
            Assertions.assertEquals(sourceSummary.toString(), destSummary, "query summaries different");

            Assertions.assertEquals(duplicateCount.get(), ((CasStorage) sync.getTarget()).getDuplicateBlobCount());
        } finally {
            if (dupClipId != null) clipIds.add(dupClipId);
            delete(sourcePool, clipIds, true);
            delete(destPool, clipIds, true);
            try {
                sourcePool.Close();
            } catch (Throwable t) {
                log.warn("failed to close source pool", t);
            }
            try {
                destPool.Close();
            } catch (Throwable t) {
                log.warn("failed to close dest pool", t);
            }
        }
    }

    @Test
    public void testVerify() throws Exception {
        FPPool sourcePool = new FPPool(connectString1);
        FPPool destPool = new FPPool(connectString2);

        // create random data (capture summary for comparison)
        StringWriter sourceSummary = new StringWriter();
        List<String> clipIds = createTestClips(sourcePool, 10240, 250, sourceSummary);

        try {
            // write clip file
            File clipFile = File.createTempFile("clip", "lst");
            clipFile.deleteOnExit();
            BufferedWriter writer = new BufferedWriter(new FileWriter(clipFile));
            for (String clipId : clipIds) {
                writer.write(clipId);
                writer.newLine();
            }
            writer.close();

            // test sync with verify
            EcsSync sync = createEcsSync(connectString1, connectString2, CAS_THREADS, true);
            sync.getSyncConfig().getOptions().setSourceListFile(clipFile.getAbsolutePath());
            sync.getSyncConfig().getOptions().setVerify(true);

            run(sync);

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

            // test verify only
            sync = createEcsSync(connectString1, connectString2, CAS_THREADS, true);
            sync.getSyncConfig().getOptions().setSourceListFile(clipFile.getAbsolutePath());
            sync.getSyncConfig().getOptions().setVerifyOnly(true);

            run(sync);

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());

            // delete clips from both
            delete(sourcePool, clipIds);
            delete(destPool, clipIds);

            // create new clips (ECS has a problem reading previously deleted and recreated clip IDs)
            clipIds = createTestClips(sourcePool, 10240, 250, sourceSummary);
            writer = new BufferedWriter(new FileWriter(clipFile));
            for (String clipId : clipIds) {
                writer.write(clipId);
                writer.newLine();
            }
            writer.close();

            // test sync+verify with failures
            sync = createEcsSync(connectString1, connectString2, CAS_THREADS, true);
            sync.getSyncConfig().getOptions().setSourceListFile(clipFile.getAbsolutePath());
            ByteAlteringFilter.ByteAlteringConfig filter = new ByteAlteringFilter.ByteAlteringConfig();
            sync.getSyncConfig().setFilters(Collections.singletonList(filter));
            sync.getSyncConfig().getOptions().setRetryAttempts(0); // retries will circumvent this test
            sync.getSyncConfig().getOptions().setVerify(true);

            run(sync);

            Assertions.assertTrue(filter.getModifiedObjects() > 0);
            Assertions.assertEquals(filter.getModifiedObjects(), sync.getStats().getObjectsFailed());
        } finally {
            // delete clips from both
            delete(sourcePool, clipIds);
            delete(destPool, clipIds);
            try {
                sourcePool.Close();
            } catch (Throwable t) {
                log.warn("failed to close source pool", t);
            }
            try {
                destPool.Close();
            } catch (Throwable t) {
                log.warn("failed to close dest pool", t);
            }
        }
    }

    @Test
    public void testSyncQuerySmallBlobs() throws Exception {
        int numClips = 250, maxBlobSize = 102400;

        FPPool sourcePool = new FPPool(connectString1);
        FPPool destPool = new FPPool(connectString2);

        // make sure both pools are empty
        Assertions.assertEquals(0, query(sourcePool).size(), "source pool contains objects");
        Assertions.assertEquals(0, query(destPool).size(), "target pool contains objects");

        // create random data (capture summary for comparison)
        StringWriter sourceSummary = new StringWriter();
        List<String> clipIds = createTestClips(sourcePool, maxBlobSize, numClips, sourceSummary);

        try {
            EcsSync sync = createEcsSync(connectString1, connectString2, CAS_THREADS, true);

            run(sync);

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(numClips, sync.getStats().getObjectsComplete());

            String destSummary = summarize(destPool, query(destPool));
            Assertions.assertEquals(sourceSummary.toString(), destSummary, "query summaries different");
        } finally {
            delete(sourcePool, clipIds);
            delete(destPool, clipIds);
            try {
                sourcePool.Close();
            } catch (Throwable t) {
                log.warn("failed to close source pool", t);
            }
            try {
                destPool.Close();
            } catch (Throwable t) {
                log.warn("failed to close dest pool", t);
            }
        }
    }

    @Test
    public void testQueryTimes() throws Exception {
        int numClips = 100, maxBlobSize = 102400;

        FPPool sourcePool = new FPPool(connectString1);
        FPPool destPool = new FPPool(connectString2);

        // make sure both pools are empty
        Assertions.assertEquals(0, query(sourcePool).size(), "source pool contains objects");
        Assertions.assertEquals(0, query(destPool).size(), "target pool contains objects");

        // create random data (capture summary for comparison)
        StringWriter sourceSummary = new StringWriter();
        List<String> clipIds = createTestClips(sourcePool, maxBlobSize, numClips, sourceSummary);

        // compensate for up to 5 seconds of clock skew
        Thread.sleep(5000);

        Calendar startTime = Calendar.getInstance(), endTime = Calendar.getInstance();
        startTime.add(Calendar.MINUTE, -10); // set start time to 10 minutes ago
        try {
            EcsSync sync = createEcsSync(connectString1, connectString2, CAS_THREADS, false);

            // set query start/end times
            CasConfig sourceConfig = (CasConfig) sync.getSyncConfig().getSource();
            sourceConfig.setQueryStartTime(Iso8601Util.format(startTime.getTime()));
            sourceConfig.setQueryEndTime(Iso8601Util.format(endTime.getTime()));

            sync.run();

            Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
            Assertions.assertEquals(numClips, sync.getStats().getObjectsComplete());

            String destSummary = summarize(destPool, query(destPool));
            Assertions.assertEquals(sourceSummary.toString(), destSummary, "query summaries different");
        } finally {
            delete(sourcePool, clipIds);
            delete(destPool, clipIds);
            try {
                sourcePool.Close();
            } catch (Throwable t) {
                log.warn("failed to close source pool", t);
            }
            try {
                destPool.Close();
            } catch (Throwable t) {
                log.warn("failed to close dest pool", t);
            }
        }
    }

    @Test
    public void testDeleteClipList() throws Exception {
        int numClips = 100, maxBlobSize = 512000;

        FPPool pool = new FPPool(connectString1);

        try {
            // get clip count before test
            int originalClipCount = query(pool).size();

            // create random data
            StringWriter sourceSummary = new StringWriter();
            List<String> clipIds = createTestClips(pool, maxBlobSize, numClips, sourceSummary);

            // verify test clips were created
            Assertions.assertEquals(originalClipCount + numClips, query(pool).size(), "wrong test clip count");

            // write clip ID file
            File clipFile = File.createTempFile("clip", "lst");
            clipFile.deleteOnExit();
            BufferedWriter writer = new BufferedWriter(new FileWriter(clipFile));
            for (String clipId : clipIds) {
                writer.write(clipId);
                writer.newLine();
            }
            writer.close();

            // construct EcsSync instance
            SyncConfig syncConfig = new SyncConfig();
            syncConfig.setOptions(new SyncOptions().withThreadCount(CAS_THREADS).withSourceListFile(clipFile.getAbsolutePath())
                    .withDeleteSource(true));
            syncConfig.setSource(new CasConfig().withConnectionString(connectString1));
            syncConfig.setTarget(new com.emc.ecs.sync.config.storage.TestConfig());

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);

            // run EcsSync
            sync.run();
            System.out.println(sync.getStats().getStatsString());

            // verify test clips were deleted
            int afterDeleteCount = query(pool).size();
            if (originalClipCount != afterDeleteCount) {
                delete(pool, clipIds);
                Assertions.fail("test clips not fully deleted");
            }
        } finally {
            try {
                pool.Close();
            } catch (Throwable t) {
                log.warn("failed to close pool", t);
            }
        }
    }

    private EcsSync createEcsSync(String connectString1, String connectString2, int threadCount, boolean enableTimings)
            throws Exception {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setSource(new CasConfig().withConnectionString(connectString1));
        syncConfig.setTarget(new CasConfig().withConnectionString(connectString2));
        syncConfig.setOptions(new SyncOptions().withThreadCount(threadCount).withRetryAttempts(1).withTimingsEnabled(enableTimings));

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);

        return sync;
    }

    protected void run(EcsSync sync) {
        System.gc();
        long startSize = Runtime.getRuntime().totalMemory();
        sync.run();
        System.gc();
        long endSize = Runtime.getRuntime().totalMemory();
        System.out.println(String.format("memory before sync: %d, after sync: %d", startSize, endSize));
        System.out.println(sync.getStats().getStatsString());
    }

    private List<String> createTestClips(FPPool pool, int maxBlobSize, int thisMany, Writer summaryWriter) throws Exception {
        return createTestClips(pool, maxBlobSize, thisMany, summaryWriter, 0, null);
    }

    private List<String> createTestClips(FPPool pool, int maxBlobSize, int thisMany, Writer summaryWriter, int chanceOfDuplicate, AtomicInteger duplicteCount) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(CAS_THREADS);

        System.out.print("Creating clips");

        List<String> clipIds = Collections.synchronizedList(new ArrayList<String>());
        List<String> summaries = Collections.synchronizedList(new ArrayList<String>());
        for (int clipIdx = 0; clipIdx < thisMany; clipIdx++) {
            service.submit(new ClipWriter(pool, clipIds, maxBlobSize, summaries, chanceOfDuplicate, duplicteCount));
        }

        service.shutdown();
        service.awaitTermination(CAS_SETUP_WAIT_MINUTES, TimeUnit.MINUTES);
        service.shutdownNow();

        if (summaryWriter != null) {
            Collections.sort(summaries);
            for (String summary : summaries) {
                summaryWriter.append(summary);
            }
        }

        System.out.println();

        return clipIds;
    }

    private void delete(FPPool pool, List<String> clipIds) throws Exception {
        delete(pool, clipIds, false);
    }

    private void delete(FPPool pool, List<String> clipIds, boolean privDelete) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(CAS_THREADS);

        System.out.print("Deleting clips");

        for (String clipId : clipIds) {
            service.submit(new ClipDeleter(pool, clipId, privDelete));
        }

        service.shutdown();
        service.awaitTermination(CAS_SETUP_WAIT_MINUTES, TimeUnit.MINUTES);
        service.shutdownNow();

        System.out.println();
    }

    private String summarize(FPPool pool, List<String> clipIds) throws Exception {
        List<String> summaries = Collections.synchronizedList(new ArrayList<String>());

        ExecutorService service = Executors.newFixedThreadPool(CAS_THREADS);

        System.out.print("Summarizing clips");

        for (String clipId : clipIds) {
            service.submit(new ClipReader(pool, clipId, summaries));
        }

        service.shutdown();
        service.awaitTermination(CAS_SETUP_WAIT_MINUTES, TimeUnit.MINUTES);
        service.shutdownNow();

        System.out.println();

        Collections.sort(summaries);
        StringBuilder out = new StringBuilder();
        for (String summary : summaries) {
            out.append(summary);
        }
        return out.toString();
    }

    private List<String> query(FPPool pool) throws Exception {
        List<String> clipIds = new ArrayList<>();

        System.out.println("Querying for clips");

        FPQueryExpression query = new FPQueryExpression();
        query.setStartTime(0);
        query.setEndTime(-1);
        query.setType(FPLibraryConstants.FP_QUERY_TYPE_EXISTING);
        FPPoolQuery poolQuery = new FPPoolQuery(pool, query);

        FPQueryResult queryResult = null;
        boolean searching = true;
        while (searching) {
            queryResult = poolQuery.FetchResult();
            switch (queryResult.getResultCode()) {

                case FPLibraryConstants.FP_QUERY_RESULT_CODE_OK:
                    clipIds.add(queryResult.getClipID());
                    break;

                case FPLibraryConstants.FP_QUERY_RESULT_CODE_INCOMPLETE:
                case FPLibraryConstants.FP_QUERY_RESULT_CODE_COMPLETE:
                case FPLibraryConstants.FP_QUERY_RESULT_CODE_PROGRESS:
                case FPLibraryConstants.FP_QUERY_RESULT_CODE_ERROR:
                    break;

                case FPLibraryConstants.FP_QUERY_RESULT_CODE_END:
                    System.out.println("End of query reached, exiting.");
                    searching = false;
                    break;

                default:
                    // Unknown error, stop running query
                    throw new RuntimeException("received error: " + queryResult.getResultCode());
            }

            queryResult.Close();
        } //while

        queryResult.Close();
        poolQuery.Close();

        return clipIds;
    }

    private String summarizeClip(FPClip clip) throws Exception {
        FPTag tag;
        List<String> tagNames = new ArrayList<>();
        List<Long> tagSizes = new ArrayList<>();
        List<byte[]> tagByteArrays = new ArrayList<>();
        while ((tag = clip.FetchNext()) != null) {
            byte[] tagBytes = null;
            if (tag.BlobExists() == 1) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                tag.BlobRead(baos);
                tagBytes = baos.toByteArray();
            }
            tagNames.add(tag.getTagName());
            tagSizes.add(tag.getBlobSize());
            tagByteArrays.add(tagBytes);
            tag.Close();
        }
        return summarizeClip(clip.getClipID(), tagNames, tagSizes, tagByteArrays);
    }

    private String summarizeClip(String clipId, List<String> tagNames, List<Long> tagSizes, List<byte[]> tagByteArrays) throws NoSuchAlgorithmException {
        StringBuilder out = new StringBuilder();
        out.append(String.format("Clip ID: %s", clipId)).append("\n");
        if (tagNames != null) {
            for (int i = 0; i < tagNames.size(); i++) {
                String md5 = "n/a";
                if (tagByteArrays.get(i) != null)
                    md5 = DatatypeConverter.printHexBinary(MessageDigest.getInstance("MD5").digest(tagByteArrays.get(i)));
                out.append(String.format("<--tag:%s--> size:%d, md5:%s", tagNames.get(i), tagSizes.get(i), md5)).append("\n");
            }
        }
        return out.toString();
    }

    private class BlobReader implements Runnable {
        private FPTag sourceTag;
        private OutputStream out;
        private boolean success = false;
        private Throwable error;

        BlobReader(FPTag sourceTag, OutputStream out) {
            this.sourceTag = sourceTag;
            this.out = out;
        }

        @Override
        public synchronized void run() {
            try {
                sourceTag.BlobRead(out);
                success = true;
            } catch (Throwable t) {
                success = false;
                error = t;
            } finally {
                // make sure you always close piped streams!
                try {
                    out.close();
                } catch (Throwable t) {
                    // ignore
                }
            }
        }

        Throwable getError() {
            return error;
        }

        boolean isSuccess() {
            return success;
        }
    }

    private class ClipWriter implements Runnable {
        private FPPool pool;
        private List<String> clipIds;
        private int maxBlobSize;
        private List<String> summaries;
        private int chanceOfDuplicate;
        private AtomicInteger duplicateCount;
        private Random random;

        ClipWriter(FPPool pool, List<String> clipIds, int maxBlobSize, List<String> summaries, int chanceOfDuplicate, AtomicInteger duplicateCount) {
            this.pool = pool;
            this.clipIds = clipIds;
            this.maxBlobSize = maxBlobSize;
            this.summaries = summaries;
            this.chanceOfDuplicate = chanceOfDuplicate;
            this.duplicateCount = duplicateCount;
            random = new Random();
        }

        @Override
        public void run() {
            try {
                FPClip clip = new FPClip(pool);
                FPTag topTag = clip.getTopTag();

                List<String> tagNames = new ArrayList<>();
                List<Long> tagSizes = new ArrayList<>();
                List<byte[]> tagByteArrays = new ArrayList<>();

                // random number of tags per clip (<= 10)
                for (int tagIdx = 0; tagIdx <= random.nextInt(10); tagIdx++) {
                    FPTag tag = new FPTag(topTag, "test_tag");
                    byte[] blobContent = null;
                    // random whether tag has blob
                    if (random.nextBoolean()) {
                        // random whether blob is duplicate
                        if (random.nextInt(100) < chanceOfDuplicate) {
                            blobContent = duplicateBlobData;
                            if (duplicateCount != null) duplicateCount.incrementAndGet();
                        } else {
                            if (maxBlobSize == 0) {
                                blobContent = new byte[0];
                            } else {
                                // random blob length (<= maxBlobSize)
                                blobContent = new byte[random.nextInt(maxBlobSize) + 1];
                                // random blob content
                                random.nextBytes(blobContent);
                            }
                        }
                        tag.BlobWrite(new ByteArrayInputStream(blobContent), FPLibraryConstants.FP_OPTION_CLIENT_CALCID);
                    }
                    tagNames.add(tag.getTagName());
                    tagSizes.add(tag.getBlobSize());
                    tagByteArrays.add(blobContent);
                    tag.Close();
                }
                topTag.Close();
                String clipId = clip.Write();
                clip.Close();

                clipIds.add(clipId);
                summaries.add(summarizeClip(clipId, tagNames, tagSizes, tagByteArrays));

                System.out.print(".");
            } catch (Exception e) {
                e.printStackTrace();
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
        }
    }

    private class ClipReader implements Runnable {
        private FPPool pool;
        private String clipId;
        private List<String> summaries;

        ClipReader(FPPool pool, String clipId, List<String> summaries) {
            this.pool = pool;
            this.clipId = clipId;
            this.summaries = summaries;
        }

        @Override
        public void run() {
            try {
                FPClip clip = new FPClip(pool, clipId, FPLibraryConstants.FP_OPEN_FLAT);
                summaries.add(summarizeClip(clip));
                clip.Close();
                System.out.print(".");
            } catch (Exception e) {
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
        }
    }

    private class ClipDeleter implements Runnable {
        private FPPool pool;
        private String clipId;
        private boolean privDelete;

        ClipDeleter(FPPool pool, String clipId) {
            this(pool, clipId, false);
        }

        ClipDeleter(FPPool pool, String clipId, boolean privDelete) {
            this.pool = pool;
            this.clipId = clipId;
            this.privDelete = privDelete;
        }

        @Override
        public void run() {
            try {
                System.out.print(".");
                if (privDelete)
                    FPClip.AuditedDelete(pool, "ecs-sync test clip deletion", clipId, FPLibraryConstants.FP_OPTION_DELETE_PRIVILEGED);
                else FPClip.Delete(pool, clipId);
            } catch (Exception e) {
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
        }
    }

    private class Producer implements Runnable {
        private byte[] data;
        private OutputStream out;

        Producer(byte[] data, OutputStream out) {
            this.data = data;
            this.out = out;
        }

        @Override
        public void run() {
            try {
                out.write(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    out.close();
                } catch (IOException e) {
                    System.out.println("could not close output stream" + e.getMessage());
                }
            }
        }
    }

    private class DelayedInputStream extends FilterInputStream {
        private long delayMs;
        private boolean firstRead = true;

        DelayedInputStream(InputStream in, long delayMs) {
            super(in);
            this.delayMs = delayMs;
        }

        @Override
        public int read() throws IOException {
            delayFirstRead();
            return super.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            delayFirstRead();
            return super.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            delayFirstRead();
            return super.read(b, off, len);
        }

        private synchronized void delayFirstRead() {
            if (firstRead) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    throw new RuntimeException("interrupted while delaying first read", e);
                }
                firstRead = false;
            }
        }
    }
}

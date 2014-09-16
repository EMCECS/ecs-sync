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
import com.emc.vipr.sync.target.CasTarget;
import com.emc.vipr.sync.test.util.SyncConfig;
import com.emc.vipr.sync.util.CasInputStream;
import com.filepool.fplibrary.*;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CasMigrationTest {
    protected static final int CLIP_OPTIONS = 0;
    protected static final int BUFFER_SIZE = 1048576; // 1MB
    protected static final int CAS_SETUP_THREADS = 20;
    protected static final int CAS_SETUP_WAIT_MINUTES = 10;

    protected String connectString1, connectString2;

    @Before
    public void setup() throws Exception {
        try {
            Properties syncProperties = SyncConfig.getProperties();

            connectString1 = syncProperties.getProperty(SyncConfig.PROP_CAS_CONNECT_STRING);
            connectString2 = syncProperties.getProperty(SyncConfig.PROP_CAS_CONNECT_STRING + "2");

            Assume.assumeNotNull(connectString1, connectString2);
        } catch (FileNotFoundException e) {
            Assume.assumeFalse("Could not load vipr.properties", true);
        }
    }

    @Test
    public void testPipedStreams() throws Exception {
        Random random = new Random();

        // test smaller than pipe buffer
        byte[] source = new byte[random.nextInt(BUFFER_SIZE) + 1];
        random.nextBytes(source);
        String md5 = Hex.encodeHexString(MessageDigest.getInstance("MD5").digest(source));
        Assert.assertEquals("MD5 mismatch", md5, pipeAndGetMd5(source));

        // test larger than pipe buffer
        source = new byte[random.nextInt(BUFFER_SIZE) + BUFFER_SIZE + 1];
        random.nextBytes(source);
        md5 = Hex.encodeHexString(MessageDigest.getInstance("MD5").digest(source));
        Assert.assertEquals("MD5 mismatch", md5, pipeAndGetMd5(source));
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

        return Hex.encodeHexString(MessageDigest.getInstance("MD5").digest(dest));
    }

    @Test
    public void testCASSingleObject() throws Exception {
        FPPool sourcePool = new FPPool(connectString1);
        FPPool targetPool = new FPPool(connectString2);

        // create clip in source (<=1MB blob size) - capture summary for comparison
        StringWriter sourceSummary = new StringWriter();
        List<String> clipIds = createTestClips(sourcePool, 1048576, 1, sourceSummary);
        String clipID = clipIds.iterator().next();

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
            Assert.assertEquals("Tag names don't match", tag.getTagName(), targetTag.getTagName());
            Assert.assertTrue("Tag " + tag.getTagName() + " attributes not equal",
                    Arrays.equals(tag.getAttributes(), targetTag.getAttributes()));

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

        Assert.assertEquals("clip IDs not equal", clipID, targetClip.Write());
        targetClip.Close();

        // check target blob data
        targetClip = new FPClip(targetPool, clipID, FPLibraryConstants.FP_OPEN_FLAT);
        Assert.assertEquals("content mismatch", sourceSummary.toString(), summarizeClip(targetClip));
        targetClip.Close();

        // delete in source and target
        FPClip.Delete(sourcePool, clipID);
        FPClip.Delete(targetPool, clipID);
    }

    //@Test
    public void deleteAllClipsFromBoth() throws Exception {
        deleteAll(new FPPool(connectString1));
        deleteAll(new FPPool(connectString2));
    }

    @Test
    public void testCli() throws Exception {
        String conString1 = "10.6.143.90,10.6.143.91?/file.pea";
        String conString2 = "10.6.143.97:3218,10.6.143.98:3218?name=e332b0c325c444438f51bfd8d25c6b55:test,secret=XvfPa442hanJ7GzQasyx+j5X9kY=";
        String clipIdFile = "clip.lst";

        String[] args = new String[]{
                "-source", "cas://" + conString1,
                "-target", "cas://" + conString2,
                "--source-clip-list", "clip.lst"
        };

        // use reflection to bootstrap ViPRSync using CLI arguments
        Method optionsMethod = ViPRSync.class.getDeclaredMethod("cliBootstrap", String[].class);
        optionsMethod.setAccessible(true);
        ViPRSync sync = (ViPRSync) optionsMethod.invoke(null, (Object) args);

        Object source = sync.getSource();
        Assert.assertNotNull("source is null", source);
        Assert.assertTrue("source is not CasSource", source instanceof CasSource);
        CasSource casSource = (CasSource) source;

        Object target = sync.getTarget();
        Assert.assertNotNull("target is null", target);
        Assert.assertTrue("target is not CasTarget", target instanceof CasTarget);
        CasTarget casTarget = (CasTarget) target;

        Assert.assertEquals("source conString mismatch", conString1, casSource.getConnectionString());
        Assert.assertEquals("source clipIdFile mismatch", clipIdFile, casSource.getClipIdFile());
        Assert.assertEquals("target conString mismatch", conString2, casTarget.getConnectionString());
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

        // write clip file
        File clipFile = File.createTempFile("clip", "lst");
        clipFile.deleteOnExit();
        BufferedWriter writer = new BufferedWriter(new FileWriter(clipFile));
        for (String clipId : clipIds) {
            writer.write(clipId);
            writer.newLine();
        }
        writer.close();

        ViPRSync sync = createViPRSync(connectString1, connectString2, 20, true);
        ((CasSource) sync.getSource()).setClipIdFile(clipFile.getAbsolutePath());

        sync.run();

        System.out.println(sync.getStatsString());

        String destSummary = summarize(destPool, clipIds);

        delete(sourcePool, clipIds);
        delete(destPool, clipIds);

        Assert.assertEquals("query summaries different", sourceSummary.toString(), destSummary);
    }

    @Test
    public void testSyncQuerySmallBlobs() throws Exception {
        int numClips = 250, maxBlobSize = 102400;

        FPPool sourcePool = new FPPool(connectString1);
        FPPool destPool = new FPPool(connectString2);

        // create random data (capture summary for comparison)
        StringWriter sourceSummary = new StringWriter();
        List<String> clipIds = createTestClips(sourcePool, maxBlobSize, numClips, sourceSummary);

        ViPRSync sync = createViPRSync(connectString1, connectString2, 20, true);

        sync.run();

        System.out.println(sync.getStatsString());

        String destSummary = summarize(destPool, query(destPool));

        delete(sourcePool, clipIds);
        delete(destPool, clipIds);

        Assert.assertEquals("query summaries different", sourceSummary.toString(), destSummary);
    }

    private ViPRSync createViPRSync(String connectString1, String connectString2, int threadCount, boolean enableTimings)
            throws Exception {
        CasSource source = new CasSource();
        source.setConnectionString(connectString1);

        CasTarget target = new CasTarget();
        target.setConnectionString(connectString2);

        ViPRSync sync = new ViPRSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setTimingsEnabled(enableTimings);
        sync.setSyncThreadCount(threadCount);

        return sync;
    }

    protected List<String> createTestClips(FPPool pool, int maxBlobSize, int thisMany, Writer summaryWriter) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(CAS_SETUP_THREADS);

        System.out.print("Creating clips");

        List<String> clipIds = Collections.synchronizedList(new ArrayList<String>());
        List<String> summaries = Collections.synchronizedList(new ArrayList<String>());
        for (int clipIdx = 0; clipIdx < thisMany; clipIdx++) {
            service.submit(new ClipWriter(pool, clipIds, maxBlobSize, summaries));
        }

        service.shutdown();
        service.awaitTermination(CAS_SETUP_WAIT_MINUTES, TimeUnit.MINUTES);
        service.shutdownNow();

        Collections.sort(summaries);
        for (String summary : summaries) {
            summaryWriter.append(summary);
        }

        System.out.println();

        return clipIds;
    }

    protected void deleteAll(FPPool pool) throws Exception {
        delete(pool, query(pool));
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

    protected String summarize(FPPool pool, List<String> clipIds) throws Exception {
        List<String> summaries = Collections.synchronizedList(new ArrayList<String>());

        ExecutorService service = Executors.newFixedThreadPool(CAS_SETUP_THREADS);

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

    protected List<String> query(FPPool pool) throws Exception {
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

    protected String summarizeClip(FPClip clip) throws Exception {
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

    protected String summarizeClip(String clipId, List<String> tagNames, List<Long> tagSizes, List<byte[]> tagByteArrays) throws NoSuchAlgorithmException {
        StringBuilder out = new StringBuilder();
        out.append(String.format("Clip ID: %s", clipId)).append("\n");
        if (tagNames != null) {
            for (int i = 0; i < tagNames.size(); i++) {
                String md5 = "n/a";
                if (tagByteArrays.get(i) != null)
                    md5 = Hex.encodeHexString(MessageDigest.getInstance("MD5").digest(tagByteArrays.get(i)));
                out.append(String.format("<--tag:%s--> size:%d, md5:%s", tagNames.get(i), tagSizes.get(i), md5)).append("\n");
            }
        }
        return out.toString();
    }

    class BlobReader implements Runnable {
        private FPTag sourceTag;
        private OutputStream out;
        private boolean success = false;
        private Throwable error;

        public BlobReader(FPTag sourceTag, OutputStream out) {
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

        public Throwable getError() {
            return error;
        }

        public boolean isSuccess() {
            return success;
        }
    }

    class ClipWriter implements Runnable {
        private FPPool pool;
        private List<String> clipIds;
        private int maxBlobSize;
        private List<String> summaries;
        private Random random;

        public ClipWriter(FPPool pool, List<String> clipIds, int maxBlobSize, List<String> summaries) {
            this.pool = pool;
            this.clipIds = clipIds;
            this.maxBlobSize = maxBlobSize;
            this.summaries = summaries;
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
                    FPTag tag = new FPTag(topTag, "test_tag_" + tagIdx);
                    byte[] blobContent = null;
                    // random whether tag has blob
                    if (random.nextBoolean()) {
                        // random blob length (<= maxBlobSize)
                        blobContent = new byte[random.nextInt(maxBlobSize) + 1];
                        // random blob content
                        random.nextBytes(blobContent);
                        tag.BlobWrite(new CasInputStream(new ByteArrayInputStream(blobContent), blobContent.length));
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
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
        }
    }

    class ClipReader implements Runnable {
        private FPPool pool;
        private String clipId;
        private List<String> summaries;

        public ClipReader(FPPool pool, String clipId, List<String> summaries) {
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

    class Producer implements Runnable {
        private byte[] data;
        private OutputStream out;

        public Producer(byte[] data, OutputStream out) {
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
}

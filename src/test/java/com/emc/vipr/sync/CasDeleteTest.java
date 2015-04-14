package com.emc.vipr.sync;

import com.emc.vipr.sync.source.CasSource;
import com.emc.vipr.sync.target.DeleteSourceTarget;
import com.emc.vipr.sync.test.SyncConfig;
import com.emc.vipr.sync.util.CasInputStream;
import com.filepool.fplibrary.*;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CasDeleteTest {
    protected static final int CAS_SETUP_THREADS = 20;
    protected static final int CAS_SETUP_WAIT_MINUTES = 10;

    protected String connectString1;

    @Before
    public void setup() throws Exception {
        try {
            Properties syncProperties = SyncConfig.getProperties();

            connectString1 = syncProperties.getProperty(SyncConfig.PROP_CAS_CONNECT_STRING);

            Assume.assumeNotNull(connectString1);
        } catch (FileNotFoundException e) {
            Assume.assumeFalse("Could not load vipr-sync.properties", true);
        }
    }

    @Test
    public void testDeleteClipList() throws Exception {
        int numClips = 100, maxBlobSize = 512000;

        FPPool pool = new FPPool(connectString1);

        // get clip count before test
        int originalClipCount = query(pool).size();

        // create random data
        StringWriter sourceSummary = new StringWriter();
        List<String> clipIds = createTestClips(pool, maxBlobSize, numClips, sourceSummary);

        // verify test clips were created
        Assert.assertEquals("wrong test clip count", originalClipCount + numClips, query(pool).size());

        // write clip ID file
        File clipFile = File.createTempFile("clip", "lst");
        clipFile.deleteOnExit();
        BufferedWriter writer = new BufferedWriter(new FileWriter(clipFile));
        for (String clipId : clipIds) {
            writer.write(clipId);
            writer.newLine();
        }
        writer.close();

        // construct ViPRSync instance
        CasSource source = new CasSource();
        source.setConnectionString(connectString1);
        source.setClipIdFile(clipFile.getAbsolutePath());

        DeleteSourceTarget target = new DeleteSourceTarget();

        ViPRSync sync = new ViPRSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setSyncThreadCount(10);

        // run ViPRSync
        sync.run();
        System.out.println(sync.getStatsString());

        // verify test clips were deleted
        int afterDeleteCount = query(pool).size();
        if (originalClipCount != afterDeleteCount) {
            delete(pool, clipIds);
            Assert.fail("test clips not fully deleted");
        }
    }

    protected List<String> createTestClips(FPPool pool, int maxBlobSize, int thisMany, Writer summaryWriter) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(CAS_SETUP_THREADS);

        System.out.print("Creating clips");

        List<String> clipIds = Collections.synchronizedList(new ArrayList<String>());
        List<String> summaries = Collections.synchronizedList(new ArrayList<String>());
        for (int clipIdx = 0; clipIdx < thisMany; clipIdx++) {
            service.submit(new ClipWriter(pool, clipIds, maxBlobSize));
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

    protected List<String> query(FPPool pool) throws Exception {
        List<String> clipIds = new ArrayList<String>();

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

    class ClipWriter implements Runnable {
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
                FPClip clip = new FPClip(pool);
                FPTag topTag = clip.getTopTag();

                // random number of tags per clip (<= 10)
                for (int tagIdx = 0; tagIdx <= random.nextInt(10); tagIdx++) {
                    FPTag tag = new FPTag(topTag, "test_tag_" + tagIdx);
                    byte[] blobContent;
                    // random whether tag has blob
                    if (random.nextBoolean()) {
                        // random blob length (<= maxBlobSize)
                        blobContent = new byte[random.nextInt(maxBlobSize) + 1];
                        // random blob content
                        random.nextBytes(blobContent);
                        tag.BlobWrite(new CasInputStream(new ByteArrayInputStream(blobContent), blobContent.length));
                    }
                    tag.Close();
                }
                topTag.Close();
                String clipId = clip.Write();
                clip.Close();

                clipIds.add(clipId);

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
}

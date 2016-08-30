package com.emc.ecs.sync.util;

import com.emc.ecs.sync.test.SyncConfig;
import com.filepool.fplibrary.*;
import org.junit.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BlobInputStreamTest {
    private String connectString;
    private List<String> clipIds = new ArrayList<>();

    @Before
    public void setup() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();

        connectString = syncProperties.getProperty(SyncConfig.PROP_CAS_CONNECT_STRING);
        Assume.assumeNotNull(connectString);
    }

    @After
    public void teardown() throws Exception {
        if (connectString != null && !clipIds.isEmpty()) {
            FPPool pool = new FPPool(connectString);
            for (String clipId : clipIds) {
                FPClip.Delete(pool, clipId);
            }
            pool.Close();
        }
    }

    @Test
    public void testStandardRead() throws Exception {
        byte[] blobContent = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".getBytes("UTF-8");

        FPPool pool = new FPPool(connectString);

        String clipId = createTestClip(pool, blobContent);

        FPClip clip = new FPClip(pool, clipId, FPLibraryConstants.FP_OPEN_FLAT);
        FPTag tag = clip.FetchNext();

        BlobInputStream blobStream = new BlobInputStream(tag, 1024);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        streamAndClose(blobStream, baos);

        tag.Close();
        clip.Close();
        pool.Close();

        Assert.assertTrue(blobStream.reader.isComplete());
        Assert.assertFalse(blobStream.reader.isFailed());
        Assert.assertFalse(blobStream.readerThread.isAlive());
    }

    @Test
    public void testReadFailure() throws Exception {
        FPPool pool = new FPPool(connectString);

        // basically we're going to create a non-blob tag, which should throw an exception when BlobRead is called
        String clipId = createTestClip(pool, null);

        FPClip clip = new FPClip(pool, clipId, FPLibraryConstants.FP_OPEN_FLAT);
        FPTag tag = clip.FetchNext();

        BlobInputStream blobStream = new BlobInputStream(tag, 1024);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            streamAndClose(blobStream, baos);
            Assert.fail("stream should have thrown exception");
        } catch (IOException e) {
            Assert.assertTrue(e.getCause() instanceof FPLibraryException);
        }

        tag.Close();
        clip.Close();
        pool.Close();

        Assert.assertEquals(0, baos.size()); // no bytes were written

        Assert.assertFalse(blobStream.reader.isComplete());
        Assert.assertTrue(blobStream.reader.isFailed());
        Assert.assertFalse(blobStream.readerThread.isAlive());
    }

    @Test
    public void testWriteFailure() throws Exception {
        byte[] blobContent = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".getBytes("UTF-8");

        FPPool pool = new FPPool(connectString);

        String clipId = createTestClip(pool, blobContent);

        FPClip clip = new FPClip(pool, clipId, FPLibraryConstants.FP_OPEN_FLAT);
        FPTag tag = clip.FetchNext();

        // 2-byte buffer ensures that the reader thread doesn't read too far ahead
        BlobInputStream blobStream = new BlobInputStream(tag, 2);

        // read 10 bytes
        byte[] buffer = new byte[10];
        int read, total = 0;
        do {
            read = blobStream.read(buffer, total, buffer.length - total);
            if (read > 0) total += read;
        } while (read != -1 && total < 10);

        // close stream early as though a write error occurred in target storage
        blobStream.close();

        tag.Close();
        clip.Close();
        pool.Close();

        Assert.assertEquals(10, total);
        Assert.assertArrayEquals(Arrays.copyOfRange(blobContent, 0, 10), buffer);

        Assert.assertFalse(blobStream.reader.isComplete());
        Assert.assertTrue(blobStream.reader.isFailed());
        Assert.assertFalse(blobStream.readerThread.isAlive());
    }

    private void streamAndClose(InputStream in, OutputStream out) throws IOException {
        try (InputStream inStream = in;
             OutputStream outStream = out) {
            byte[] buffer = new byte[1024];
            int read;
            while (((read = inStream.read(buffer)) != -1)) {
                outStream.write(buffer, 0, read);
            }
        }
    }

    // create 1 clip with 1 blob tag
    private String createTestClip(FPPool pool, byte[] blobContent) throws Exception {
        FPClip clip = new FPClip(pool, "test-clip");
        FPTag topTag = clip.getTopTag();

        FPTag tag = new FPTag(topTag, "test-tag");
        if (blobContent != null) tag.BlobWrite(new ByteArrayInputStream(blobContent));
        tag.Close();

        topTag.Close();
        String clipId = clip.Write();
        clip.Close();

        clipIds.add(clipId);

        return clipId;
    }
}

package com.emc.ecs.sync.util;

import com.emc.ecs.sync.filter.ClamAvFilter;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test the ClamAV filter.
 */
public class ClamAvInputStreamTest {
    private static final Logger log = LoggerFactory.getLogger(ClamAvInputStreamTest.class);

    private static boolean clamRunning = false;
    private static String host;
    private static int port;
    private static long maxScanSize;

    @BeforeClass
    public static void clamdConfig() {
        // See if clamd is running before we even try.
        host = System.getProperty("clamd.host", "localhost");
        port = Integer.parseInt(System.getProperty("clamd.port", "3310"));
        maxScanSize = Long.parseLong(System.getProperty("clamd.max-size", "26214400")); // 25MB

        try {
            ClamAvFilter.clamPing(host, port);
            clamRunning = true;
        } catch(Exception e) {
            log.debug("Error connecting to ClamAV, skipping tests: " + e);
        }
    }

    @Test
    public void testCleanText() throws Exception {
        Assume.assumeTrue(clamRunning);
        InputStream in = new ClamAvInputStream(new ByteArrayInputStream("Hello World".getBytes()), host, port, maxScanSize);
        AtomicLong counter = new AtomicLong(0);
        drain(in, counter);
        in.close();
        Assert.assertEquals("Wrong byte count", 11L, counter.longValue());
    }

    @Test
    public void testVirus() throws Exception {
        Assume.assumeTrue(clamRunning);
        AtomicLong counter = new AtomicLong(0);
        try {
            InputStream in = new ClamAvInputStream(new ByteArrayInputStream(CLAM_MAIL.getBytes()), host, port, maxScanSize);
            drain(in, counter);
            in.close();
        } catch(Exception e) {
            Assert.assertTrue("Wrong class", e.getClass().equals(RuntimeException.class));
            Assert.assertEquals("Wrong error message", "ClamAV error: stream: Clamav.Test.File-6 FOUND", e.getMessage());
        }
        Assert.assertEquals("Wrong byte count", 1337L, counter.longValue());
    }

    /**
     * Make sure we still stream data after the 25MB cutoff.
     */
    @Test
    public void testBigData() throws Exception {
        Assume.assumeTrue(clamRunning);
        AtomicLong counter = new AtomicLong(0);
        try {
            byte[] buffer = new byte[64 * 1024 * 1024];
            InputStream in = new ClamAvInputStream(new ByteArrayInputStream(buffer), host, port, maxScanSize);
            drain(in, counter);
            in.close();
            Assert.assertEquals("Wrong byte count", 64 * 1024 * 1024, counter.longValue());
        } catch(Exception e) {
            log.error("Error after {} bytes", counter.longValue());
            throw e;
        }
    }

    private void drain(InputStream in, AtomicLong count) throws IOException {
        byte[] buffer = new byte[4096];

        int c = 0;
        while((c = in.read(buffer)) != -1) {
            count.addAndGet(c);
        }
    }

    public static final String CLAM_MAIL = "From: ClamAV\n" +
            "To: ClamAV\n" +
            "Subject: ClamAV Test File\n" +
            "Message-ID: <20080603232833.1aeaf8f1@ClamAV>\n" +
            "Organization: ClamAV\n" +
            "Mime-Version: 1.0\n" +
            "Content-Type: multipart/mixed; boundary=\"MP_/6OvrPH9HEPZRUCVu6uT=Fey\"\n" +
            "\n" +
            "--MP_/6OvrPH9HEPZRUCVu6uT=Fey\n" +
            "Content-Type: text/plain; charset=US-ASCII\n" +
            "Content-Transfer-Encoding: 7bit\n" +
            "Content-Disposition: inline\n" +
            "\n" +
            "This is a ClamAV test file with embedded clam.exe\n" +
            "--MP_/6OvrPH9HEPZRUCVu6uT=Fey\n" +
            "Content-Type: application/x-ms-dos-executable; name=clam.exe\n" +
            "Content-Transfer-Encoding: base64\n" +
            "Content-Disposition: attachment; filename=clam.exe\n" +
            "\n" +
            "TVpQAAIAAAAEAA8A//8AALgAAAAhAAAAQAAaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n" +
            "AAAAAAEAALtxEEAAM8BQUIvzU1NQsClAMARmrHn5ujEAeA2tUP9mcA4fvjEA6eX/tAnNIbRMzSFi\n" +
            "DAoBAnB2FwIeTgwEL9rMEAAAAAAAAAAAAAAAAAAAwBAAAIAQAAAAAAAAAAAAAAAAAADaEAAA9BAA\n" +
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAS0VSTkVMMzIuRExMAABFeGl0UHJvY2VzcwBVU0VSMzIuRExM\n" +
            "AENMQU1lc3NhZ2VCb3hBAOYQAAAAAAAAPz8/P1BFAABMAQEAYUNhQgAAAAAAAAAA4ACOgQsBAhkA\n" +
            "BAAAAAYAAAAAAABAEAAAABAAAEAAAAAAAEAAABAAAAACAAABAAAAAAAAAAMACgAAAAAAACAAAAAE\n" +
            "AAAAAAAAAgAAAAAAEAAAIAAAAAAQAAAQAAAAAAAAEAAAAAAAAAAAAAAAhBAAAIAAAAAAAAAAAAAA\n" +
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n" +
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAW0NMQU1BVl0A\n" +
            "EAAAABAAAAACAAABAAAAAAAAAAAAAAAAAAAAAAAAwA==\n" +
            "\n" +
            "--MP_/6OvrPH9HEPZRUCVu6uT=Fey--\n";
}

package com.emc.ecs.sync.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * FilterInputStream that scans the bytes passing through it for viruses using ClamAV.
 */
public class ClamAvInputStream extends FilterInputStream {
    private static final Logger log = LoggerFactory.getLogger(ClamAvInputStream.class);

    private String host;
    private int port;
    private long maxScanSize;
    private long bytesScanned;
    private boolean scanComplete;
    private BufferedReader clamIn;
    private DataOutputStream clamOut;
    private Socket clamSock;

    /**
     * Creates a new ClamAV Input Stream Filter
     * @param parent InputStream to scan
     * @param host Host or IP running clamd
     * @param port Port number clamd is running on
     * @param maxScanSize Maximum number of bytes clamd will accept.
     */
    public ClamAvInputStream(InputStream parent, String host, int port, long maxScanSize) {
        super(parent);
        this.host = host;
        this.port = port;
        this.maxScanSize = maxScanSize;
        scanComplete = false;
        bytesScanned = 0L;

        // Note that ClamAV has a 5 second timeout for the first command on the stream so we wait to establish the
        // socket connection until later.
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int c = in.read(b, off, len);

        if(c == -1) {
            completeScan();
        }

        scan(b, off, c);

        return c;
    }

    private void completeScan() throws IOException {
        if(scanComplete) return;

        clamOut.writeInt(0);
        String response = clamIn.readLine();
        log.debug("ClamAV Response: {}", response);

        clamClose();

        scanComplete = true;

        if(!response.endsWith("OK")) {
            throw new RuntimeException("ClamAV error: " + response);
        }
    }

    /**
     * Scans the given byte array.
     * @param b the byte array
     * @param off offset into the array
     * @param len number of bytes to scan
     * @throws IOException if an error occurs communicating with ClamAV.
     */
    private void scan(byte[] b, int off, int len) throws IOException {
        if(scanComplete) {
            return;
        }
        if(bytesScanned >= maxScanSize) {
            completeScan();
            return;
        }
        if(bytesScanned + len > maxScanSize) {
            scan(b, off, (int)(maxScanSize - bytesScanned));
            return;
        }

        if(clamSock == null) {
            clamConnect();
        }

        clamOut.writeInt(len);
        clamOut.write(b, off, len);
        bytesScanned += len;
    }

    /**
     * Connect to ClamAV
     */
    private void clamConnect() throws IOException {
        clamSock = new Socket(host, port);
        clamIn = new BufferedReader(new InputStreamReader(clamSock.getInputStream()));
        clamOut = new DataOutputStream(clamSock.getOutputStream());
        clamOut.writeBytes("nINSTREAM\n");
    }

    @Override
    public int read() throws IOException {
        // .. unless you want to kill performance.
        throw new RuntimeException("Don't ever call read() on an InputStream.");
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public void close() throws IOException {
        super.close();
        clamClose();
    }

    /**
     * Safely close the connection to ClamAV
     */
    private void clamClose() {
        if(clamIn != null) {
            try {
                clamIn.close();
            } catch (IOException e) {
                // Ignore
            }
            clamIn = null;
        }
        if(clamOut != null) {
            try {
                clamOut.close();
            } catch (IOException e) {
                // Ignore
            }
            clamOut = null;
        }
        if(clamSock != null) {
            try {
                clamSock.close();
            } catch (IOException e) {
                // Ignore
            }
            clamSock = null;
        }
    }
}

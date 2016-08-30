package com.emc.ecs.sync.util;

import com.emc.object.util.ProgressListener;
import com.emc.object.util.ProgressOutputStream;
import com.filepool.fplibrary.FPTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class BlobInputStream extends CountingInputStream {
    private static final Logger log = LoggerFactory.getLogger(BlobInputStream.class);

    private FPTag tag;
    BlobReader reader;
    Thread readerThread;

    public BlobInputStream(FPTag tag, int bufferSize) throws IOException {
        this(tag, bufferSize, null);
    }

    public BlobInputStream(FPTag tag, int bufferSize, ProgressListener listener) throws IOException {
        super(null);
        this.tag = tag;

        // piped streams and a reader task are necessary because of the odd stream handling in the CAS JNI wrapper
        PipedInputStream pin = new PipedInputStream(bufferSize);
        PipedOutputStream pout = new PipedOutputStream(pin);

        try {
            in = new DigestInputStream(pin, MessageDigest.getInstance("md5"));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("could not initialize MD5 digest", e);
        }

        OutputStream out = pout;
        if (listener != null) out = new ProgressOutputStream(out, listener);

        reader = new BlobReader(out);
        readerThread = new Thread(reader);
        readerThread.start();
    }

    @Override
    public int read() throws IOException {
        checkReader();
        int result = super.read();
        checkReader(); // also check after read in case we were blocked
        return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkReader();
        int result = super.read(b, off, len);
        checkReader(); // also check after read in case we were blocked
        return result;
    }

    private void checkReader() throws IOException {
        if (reader.isFailed()) throw new IOException("blob reader failed", reader.getError());
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {

            // if the reader is active, closing the pipe (above) will throw an exception in PipedOutputStream.write
            // if it is waiting for buffer space however, it will need to be interrupted or it will be frozen indefinitely
            if (!reader.isComplete() && !reader.isFailed())
                readerThread.interrupt();

            // if the reader is complete, this does nothing; if close was called early, this will wait until the reader
            // thread is notified of the close (an IOException will be thrown from PipedOutputStream.write)
            try {
                readerThread.join();
            } catch (Throwable t) {
                log.warn("could not join reader thread", t);
            }
        }
    }

    public byte[] getMd5Digest() {
        if (!(in instanceof DigestInputStream)) throw new UnsupportedOperationException("MD5 checksum is not enabled");
        if (!isClosed()) throw new UnsupportedOperationException("cannot get MD5 until stream is closed");
        return ((DigestInputStream) in).getMessageDigest().digest();
    }

    class BlobReader implements Runnable {
        private OutputStream out;
        private volatile boolean complete = false;
        private volatile boolean failed = false;
        private volatile Throwable error;

        BlobReader(OutputStream out) {
            this.out = out;
        }

        @Override
        public synchronized void run() {
            try (OutputStream outputStream = out) {
                tag.BlobRead(outputStream);
                complete = true;
            } catch (Throwable t) {
                failed = true;
                error = t;
            }
        }

        public boolean isComplete() {
            return complete;
        }

        public boolean isFailed() {
            return failed;
        }

        public Throwable getError() {
            return error;
        }
    }
}

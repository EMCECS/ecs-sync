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
package com.emc.ecs.sync.storage.cas;

import com.emc.ecs.sync.util.EnhancedInputStream;
import com.emc.object.util.ProgressListener;
import com.emc.object.util.ProgressOutputStream;
import com.filepool.fplibrary.FPLibraryException;
import com.filepool.fplibrary.FPStreamInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class BlobInputStream extends EnhancedInputStream implements FPStreamInterface {
    private static final Logger log = LoggerFactory.getLogger(BlobInputStream.class);

    private static long getBlobSize(CasTag tag) {
        try {
            return tag.getBlobSize();
        } catch (FPLibraryException e) {
            throw new RuntimeException("error getting blob size: " + CasStorage.summarizeError(e), e);
        }
    }

    private CasTag tag;
    private int bufferSize;
    private boolean drainOnError;
    private BlobReader blobReader;
    private Thread readerThread;
    private Future readFuture;
    private byte[] digest;

    public BlobInputStream(CasTag tag, int bufferSize) throws IOException {
        this(tag, bufferSize, null, null);
    }

    public BlobInputStream(CasTag tag, int bufferSize, ExecutorService readExecutor) throws IOException {
        this(tag, bufferSize, null, readExecutor);
    }

    public BlobInputStream(CasTag tag, int bufferSize, ProgressListener listener) throws IOException {
        this(tag, bufferSize, listener, null);
    }

    public BlobInputStream(CasTag tag, int bufferSize, ProgressListener listener, ExecutorService readExecutor) throws IOException {
        super(null, getBlobSize(tag));
        this.tag = tag;
        this.bufferSize = bufferSize;

        // piped streams and a blobReader task are necessary because of the odd stream handling in the CAS JNI wrapper
        PipedInputStream pin = new PipedInputStream(bufferSize);
        PipedOutputStream pout = new PipedOutputStream(pin);

        try {
            in = new DigestInputStream(pin, MessageDigest.getInstance("md5"));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("could not initialize MD5 digest", e);
        }

        OutputStream out = pout;
        if (listener != null) out = new ProgressOutputStream(out, listener);

        blobReader = new BlobReader(out, getSize());
        if (readExecutor != null) {
            readFuture = readExecutor.submit(blobReader);
        } else {
            readerThread = new Thread(blobReader);
            readerThread.setDaemon(true);
            readerThread.start();
        }
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
        if (blobReader.isFailed()) throw new IOException("blob reader failed", blobReader.getError());
    }

    @Override
    public void close() throws IOException {
        try {

            // if requested, completely read the source blob before closing the stream
            if (drainOnError && !blobReader.isComplete() && !blobReader.isFailed()) {
                try {
                    byte[] buffer = new byte[32 * 1024];
                    int c = 0;
                    while (c != -1) {
                        c = read(buffer);
                    }
                } catch (Throwable t) {
                    log.warn("[" + tag.getClipId() + "]: could not drain source blob before closing early", t);
                }
            }

            // NOTE: closing the stream *before* the reader thread is finished will throw an exception in
            // PipedOutputStream.write
            // - if the reader thread is active, the exception will be immediate
            // - if it is waiting for buffer space, it will get an exception on the next poll (it polls every second)
            super.close();

        } finally {

            // if the blobReader is complete, this does nothing; if close was called early, this will wait until the blobReader
            // thread is notified of the close (an IOException will be thrown from PipedOutputStream.write)
            try {
                if (readerThread != null) readerThread.join();
                else readFuture.get();
            } catch (Throwable t) {
                if (blobReader.isFailed() && blobReader.getError() instanceof IOException
                        && "Pipe closed".equals(blobReader.getError().getMessage()))
                    log.warn("[" + tag.getClipId() + "]: blob stream was closed early");
                else log.warn("[" + tag.getClipId() + "]: could not join blobReader thread", t);
            }

            // save MD5 so we can GC the piped stream buffer
            try {
                getMd5Digest();
            } catch (Throwable t) {
                log.warn("[" + tag.getClipId() + "]: could not get MD5 from stream", t);
            }
            in = null;
        }
    }

    @Override
    public long getStreamLength() {
        return getSize();
    }

    @Override
    public boolean FPMarkSupported() {
        return markSupported();
    }

    @Override
    public void FPMark() {
        mark(bufferSize);
    }

    @Override
    public void FPReset() throws IOException {
        reset();
    }

    public synchronized byte[] getMd5Digest() {
        if (digest == null) {
            if (!(in instanceof DigestInputStream))
                throw new UnsupportedOperationException("MD5 checksum is not enabled");
            if (!isClosed()) throw new UnsupportedOperationException("cannot get MD5 until stream is closed");
            digest = ((DigestInputStream) in).getMessageDigest().digest();
        }
        return digest;
    }

    public boolean isDrainOnError() {
        return drainOnError;
    }

    public void setDrainOnError(boolean drainOnError) {
        this.drainOnError = drainOnError;
    }

    private class BlobReader implements Runnable {
        private OutputStream out;
        private long size;
        private volatile boolean complete = false;
        private volatile boolean failed = false;
        private volatile Throwable error;

        BlobReader(OutputStream out, long size) {
            this.out = out;
            this.size = size;
        }

        @Override
        public synchronized void run() {
            try (OutputStream outputStream = new CasOutputStream(out, size)) {
                tag.BlobRead(outputStream);
                complete = true;
            } catch (Throwable t) {
                failed = true;
                error = t;
            } finally {
                out = null; // free reference for GC
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

    private class CasOutputStream extends FilterOutputStream implements FPStreamInterface {
        private long size;

        public CasOutputStream(OutputStream out, long size) {
            super(out);
            this.size = size;
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public long getStreamLength() {
            return size;
        }

        @Override
        public boolean FPMarkSupported() {
            return markSupported();
        }

        @Override
        public void FPMark() {
            mark(bufferSize);
        }

        @Override
        public void FPReset() throws IOException {
            reset();
        }
    }
}

/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.util;

import com.emc.object.util.ProgressInputStream;
import com.emc.object.util.ProgressListener;
import com.emc.object.util.ProgressOutputStream;
import com.filepool.fplibrary.FPLibraryException;
import com.filepool.fplibrary.FPTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ClipTag implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ClipTag.class);

    private FPTag tag;
    private int tagNum;
    private int bufferSize;
    private boolean blobAttached = false;
    private CasInputStream cin;
    private long bytesRead = 0;
    private byte[] md5Digest;
    private ProgressListener listener;

    public ClipTag(FPTag tag, int tagNum, int bufferSize, ProgressListener listener) throws FPLibraryException {
        this.tag = tag;
        this.tagNum = tagNum;
        this.bufferSize = bufferSize;
        this.listener = listener;

        int blobStatus = tag.BlobExists();
        if (blobStatus == 1) {
            blobAttached = true;
        } else {
            // no data attached to this tag
            if (blobStatus != -1)
                log.warn("[" + tag.getClipRef().getClipID() + "." + tagNum + "]: blob unavailable (status=" + blobStatus + ")");
        }
    }

    /**
     * Copies the blob contents to the target tag. This method handles reader synchronization.
     */
    public void writeToTag(final FPTag targetTag, final ProgressListener targetProgress) throws IOException, FPLibraryException, InterruptedException {
        processBlob(new BlobProcessor() {
            @Override
            public void process(InputStream stream) throws IOException, FPLibraryException {
                if (targetProgress != null) stream = new ProgressInputStream(stream, targetProgress);
                targetTag.BlobWrite(stream);
            }
        });

        log.debug("Blob write complete, tag={} bytes={} md5={}", tagNum, bytesRead, md5Digest);
    }

    /**
     * Copies the blob contents to the target stream.   This method handles reader synchronization, but does not close
     * the target stream.
     */
    public void writeToStream(final OutputStream out) throws IOException, FPLibraryException, InterruptedException {
        processBlob(new BlobProcessor() {
            @Override
            public void process(InputStream stream) throws IOException {
                byte[] buffer = new byte[128 * 1024]; // 128k buffer
                int read;
                while (((read = stream.read(buffer)) != -1)) {
                    out.write(buffer, 0, read);
                }
            }
        });
    }

    protected void processBlob(BlobProcessor blobProcessor) throws IOException, FPLibraryException, InterruptedException {
        if (!blobAttached) throw new UnsupportedOperationException("this tag has no blob data");
        if (tag == null) throw new UnsupportedOperationException("this tag has been closed");

        // piped streams and a reader task are necessary because of the odd stream handling in the CAS JNI wrapper
        PipedInputStream pin = new PipedInputStream(bufferSize);
        PipedOutputStream pout = new PipedOutputStream(pin);
        cin = new CasInputStream(pin, tag.getBlobSize(), true);

        OutputStream out = pout;
        if (listener != null) out = new ProgressOutputStream(out, listener);
        BlobReader reader = new BlobReader(out);
        Thread readerThread = new Thread(reader);

        readerThread.start();

        try (InputStream blobStream = cin) {
            blobProcessor.process(blobStream);
        } catch (Throwable t) {

            // There was a write error. Since we can't stop the reader thread, drain the pipe stream so we can close it
            log.warn("Error processing blob data. Trying to drain reader stream");
            try {
                SyncUtil.consumeAndCloseStream(cin);
                log.info("blob stream drained");
            } catch (Throwable t2) {
                log.warn("could not drain blob stream: " + t2);
            }

            if (t instanceof IOException) throw (IOException) t;
            if (t instanceof FPLibraryException) throw (FPLibraryException) t;
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            throw new RuntimeException(t);
        } finally {
            bytesRead = cin.getBytesRead();
            try {
                md5Digest = cin.getMd5Digest();
            } catch (Throwable t) {
                log.warn("could not get MD5 digest: " + t);
            }
            cin = null; // lose the piped stream reference (and the large buffer too)

            // if we've finished writing, the reader thread should be finished too, but just in case...
            readerThread.join();
        }

        if (!reader.isSuccess())
            throw new RuntimeException("blob reader did not complete successfully", reader.getError());
    }

    public long getBytesRead() {
        if (cin == null) return bytesRead;
        return cin.getBytesRead();
    }

    public byte[] getMd5Digest(boolean forceRead) {
        if (md5Digest == null && forceRead) {
            if (cin != null) throw new UnsupportedOperationException("wait for the tag to be fully streamed");
            try {
                processBlob(new BlobProcessor() {
                    @Override
                    public void process(InputStream stream) throws IOException {
                        SyncUtil.consumeAndCloseStream(stream);
                    }
                });
            } catch (RuntimeException e) {
                throw e;
            } catch (FPLibraryException e) {
                throw new RuntimeException(CasUtil.summarizeError(e), e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return md5Digest;
    }

    public FPTag getTag() {
        return tag;
    }

    @Override
    public void close() {
        try {
            if (tag != null) tag.Close();
        } catch (FPLibraryException e) {
            throw new RuntimeException(CasUtil.summarizeError(e), e);
        } finally {
            tag = null; // remove reference to free memory
        }
    }

    public int getTagNum() {
        return tagNum;
    }

    public boolean isBlobAttached() {
        return blobAttached;
    }

    protected class BlobReader implements Runnable {
        private OutputStream out;
        private boolean success = false;
        private Throwable error;

        public BlobReader(OutputStream out) {
            this.out = out;
        }

        @Override
        public synchronized void run() {
            try (OutputStream outputStream = out) {
                tag.BlobRead(outputStream);
                success = true;
            } catch (Throwable t) {
                success = false;
                error = t;
            }
        }

        public Throwable getError() {
            return error;
        }

        public boolean isSuccess() {
            return success;
        }
    }
}

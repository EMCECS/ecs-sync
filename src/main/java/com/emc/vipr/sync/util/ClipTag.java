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
package com.emc.vipr.sync.util;

import com.filepool.fplibrary.FPLibraryException;
import com.filepool.fplibrary.FPTag;
import org.apache.log4j.Logger;

import java.io.*;

public class ClipTag {
    private static final Logger l4j = Logger.getLogger(ClipTag.class);

    private FPTag tag;
    private int tagNum;
    private int bufferSize;
    private boolean blobAttached = false;
    long bytesRead = 0;

    public ClipTag(FPTag tag, int tagNum, int bufferSize) throws FPLibraryException {
        this.tag = tag;
        this.tagNum = tagNum;
        this.bufferSize = bufferSize;

        int blobStatus = tag.BlobExists();
        if (blobStatus == 1) {
            blobAttached = true;
        } else {
            // no data attached to this tag
            if (blobStatus != -1)
                l4j.warn("[" + tag.getClipRef().getClipID() + "." + tagNum + "]: blob unavailable (status=" + blobStatus + ")");
        }
    }

    /**
     * Copies the blob contents to the target tag. This method handles reader synchronization.
     */
    public void writeToTag(FPTag targetTag) throws IOException, FPLibraryException, InterruptedException {
        if (!blobAttached) throw new UnsupportedOperationException("this tag has no blob data");

        // piped streams and a reader task are necessary because of the odd stream handling in the CAS JNI wrapper
        PipedInputStream pin = new PipedInputStream(bufferSize);
        PipedOutputStream pout = new PipedOutputStream(pin);
        CasInputStream cin = new CasInputStream(pin, tag.getBlobSize());

        BlobReader reader = new BlobReader(tag, pout);
        Thread readerThread = new Thread(reader);

        readerThread.start();

        boolean writeComplete = false;
        try {
            targetTag.BlobWrite(cin);
            writeComplete = true;
        } finally {
            if(!writeComplete) {
                // There was a write error.  Drain the stream so we can close down the Pipe thread
                l4j.warn("Write incomplete. Trying to drain pipe");
                safeDrain(cin);

                l4j.warn("Trying to join pipe thread");
                try {
                    readerThread.join();
                } catch(Throwable t) {
                    l4j.warn("Unable to join pipe thread: " + t);
                }
            }

            // make sure you always close piped streams!
            bytesRead = cin.getBytesRead();
            safeClose(cin);
        }

        // wait until the reader is finished
        readerThread.join();

        if (!reader.isSuccess())
            throw new RuntimeException("blob reader did not complete successfully", reader.getError());
    }

    /**
     * Since we use a PipedInputStream to translate CAS's OutputStream into an InputStream, we need to ensure
     * it's fully closed before we proceed to ensure the pipe thread shuts down so we can release the native
     * FPTag object.
     * @param cin the CasInputStream to drain.
     */
    private void safeDrain(CasInputStream cin) {
        try {
            byte[] buffer = new byte[32768];
            int c = 0;
            while(true) {
                c = cin.read(buffer);
                if(c == -1) {
                    break;
                }
            }
            l4j.info("CasInputStream drained.");
        } catch (Throwable t) {
            l4j.warn("Error draining CasInputStream: " + t);
        }
    }

    /**
     * Copies the blob contents to the target stream.   This method handles reader synchronization, but does not close
     * the target stream.
     */
    public void writeToStream(OutputStream out) throws IOException, FPLibraryException, InterruptedException {
        if (!blobAttached) throw new UnsupportedOperationException("this tag has no blob data");

        // piped streams and a reader task are necessary because of the odd stream handling in the CAS JNI wrapper
        PipedInputStream pin = new PipedInputStream(bufferSize);
        PipedOutputStream pout = new PipedOutputStream(pin);
        CasInputStream cin = new CasInputStream(pin, tag.getBlobSize());

        BlobReader reader = new BlobReader(tag, pout);
        Thread readerThread = new Thread(reader);

        readerThread.start();

        boolean writeComplete = false;
        try {
            byte[] buffer = new byte[65536]; // 64k buffer
            int read;
            while (((read = cin.read(buffer)) != -1)) {
                out.write(buffer, 0, read);
            }
            writeComplete = true;
        } finally {
            if(!writeComplete) {
                // There was a write error.  Drain the stream so we can close down the Pipe thread
                l4j.warn("Write incomplete. Trying to drain pipe");
                safeDrain(cin);

                l4j.warn("Trying to join pipe thread");
                try {
                    readerThread.join();
                } catch(Throwable t) {
                    l4j.warn("Unable to join pipe thread: " + t);
                }
            }

            // make sure you always close piped streams!
            bytesRead = cin.getBytesRead();
            safeClose(cin);
        }

        // wait until the reader is finished
        readerThread.join();

        if (!reader.isSuccess())
            throw new RuntimeException("blob reader did not complete successfully", reader.getError());
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public FPTag getTag() {
        return tag;
    }

    public int getTagNum() {
        return tagNum;
    }

    public boolean isBlobAttached() {
        return blobAttached;
    }

    protected void safeClose(Closeable closeable) {
        try {
            if (closeable != null) closeable.close();
        } catch (Throwable t) {
            l4j.warn("could not close resource", t);
        }
    }

    protected class BlobReader implements Runnable {
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
                safeClose(out);
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

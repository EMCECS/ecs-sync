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
package com.emc.atmos.sync.plugins.cas;

import com.filepool.fplibrary.FPLibraryException;
import com.filepool.fplibrary.FPTag;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URISyntaxException;

public class ClipTag {
    private static final Logger l4j = Logger.getLogger(ClipTag.class);

    private FPTag tag;
    private int tagNum;
    private int bufferSize;
    private boolean blobAttached = false;
    private CasInputStream cin;

    public ClipTag(FPTag tag, int tagNum, int bufferSize)
            throws URISyntaxException, FPLibraryException, IOException {
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
        cin = new CasInputStream(pin, tag.getBlobSize());

        BlobReader reader = new BlobReader(tag, pout);
        Thread readerThread = new Thread(reader);

        readerThread.start();

        try {
            targetTag.BlobWrite(cin);
        } finally {
            // make sure you always close piped streams!
            try {
                cin.close();
            } catch (Throwable t) {
                l4j.warn("could not close input stream", t);
            }
        }

        // wait until the reader is finished
        readerThread.join();

        if (!reader.isSuccess())
            throw new RuntimeException("blob reader did not complete successfully", reader.getError());
    }

    /**
     * Copies the blob contents to the target stream. This method handles reader synchronization, but does not close
     * the target stream.
     */
    public void writeToStream(OutputStream out) throws IOException, FPLibraryException, InterruptedException {
        if (!blobAttached) throw new UnsupportedOperationException("this tag has no blob data");

        // piped streams and a reader task are necessary because of the odd stream handling in the CAS JNI wrapper
        PipedInputStream pin = new PipedInputStream(bufferSize);
        PipedOutputStream pout = new PipedOutputStream(pin);
        cin = new CasInputStream(pin, tag.getBlobSize());

        BlobReader reader = new BlobReader(tag, pout);
        Thread readerThread = new Thread(reader);

        readerThread.start();

        try {
            byte[] buffer = new byte[65536]; // 64k buffer
            int read;
            while (((read = cin.read(buffer)) != -1)) {
                out.write(buffer, 0, read);
            }
        } finally {
            // make sure you always close piped streams!
            try {
                cin.close();
            } catch (Throwable t) {
                l4j.warn("could not close input stream", t);
            }
        }

        // wait until the reader is finished
        readerThread.join();

        if (!reader.isSuccess())
            throw new RuntimeException("blob reader did not complete successfully", reader.getError());
    }

    public long getBytesRead() {
        if (cin == null)
            return 0;
        return cin.getBytesRead();
    }

    protected FPTag getTag() {
        return tag;
    }

    protected int getTagNum() {
        return tagNum;
    }

    public boolean isBlobAttached() {
        return blobAttached;
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
                try {
                    out.close();
                } catch (Throwable t) {
                    l4j.warn("could not close output stream", t);
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
}

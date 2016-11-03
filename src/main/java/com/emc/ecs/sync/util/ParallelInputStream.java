/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ParallelInputStream extends FilterInputStream {
    private static final Logger log = LoggerFactory.getLogger(ParallelInputStream.class);

    ParallelReader reader;
    Thread readerThread;

    public ParallelInputStream(InputStream source, int bufferSize) throws IOException {
        this(source, bufferSize, null);
    }

    public ParallelInputStream(InputStream source, int bufferSize, ProgressListener sourceListener) throws IOException {
        super(null);

        // create a piped stream (ring buffer) -- the readerThread will be the write side and calling code
        // will be the read side
        PipedInputStream pin = new PipedInputStream(bufferSize);
        PipedOutputStream pout = new PipedOutputStream(pin);

        in = pin;

        if (sourceListener != null) source = new ProgressInputStream(source, sourceListener);

        reader = new ParallelReader(source, pout);
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
        if (reader.isFailed()) throw new IOException("reader failed", reader.getError());
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

    class ParallelReader implements Runnable {
        private InputStream in;
        private OutputStream out;
        private volatile boolean complete = false;
        private volatile boolean failed = false;
        private volatile Throwable error;

        ParallelReader(InputStream in, OutputStream out) {
            this.in = in;
            this.out = out;
        }

        @Override
        public synchronized void run() {
            try (OutputStream outputStream = out) { // make sure the write-side of the piped stream is always closed
                byte[] chunk = new byte[128 * 1024];
                int read;
                while ((read = in.read(chunk)) != -1) {
                    outputStream.write(chunk, 0, read);
                }
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

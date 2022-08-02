/*
 * Copyright (c) 2016-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.util;

import com.emc.object.util.ProgressListener;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class ParallelInputStreamTest {
    @Test
    public void testStandardRead() throws Exception {
        byte[] content = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".getBytes("UTF-8");

        ParallelInputStream pStream = new ParallelInputStream(new ByteArrayInputStream(content), 1024);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        streamAndClose(pStream, baos);

        Assertions.assertTrue(pStream.reader.isComplete());
        Assertions.assertFalse(pStream.reader.isFailed());
        Assertions.assertFalse(pStream.readerThread.isAlive());
    }

    @Test
    public void testReadFailure() throws Exception {
        byte[] content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ!abcdefghijklmnopqrstuvwxyz0123456789".getBytes("UTF-8");
        String message = "bang";

        // use 2-byte buffer to ensure the reader thread doesn't fail too early
        InputStream rawStream = new FailingInputStream(new ByteArrayInputStream(content), '!', message);
        ParallelInputStream pStream = new ParallelInputStream(rawStream, 2);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            streamAndClose(pStream, baos);
            Assertions.fail("stream should have thrown exception");
        } catch (IOException e) {
            Assertions.assertEquals("bang", e.getCause().getMessage());
        }

        Assertions.assertEquals(24, baos.size()); // should fail {bufferSize} characters before the bang
        Assertions.assertFalse(pStream.reader.isComplete());
        Assertions.assertTrue(pStream.reader.isFailed());
        Assertions.assertFalse(pStream.readerThread.isAlive());
    }

    @Test
    public void testWriteFailure() throws Exception {
        byte[] content = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".getBytes("UTF-8");

        final AtomicLong readCount = new AtomicLong();
        ProgressListener readListener = new ProgressListener() {
            @Override
            public void progress(long completed, long total) {
            }

            @Override
            public void transferred(long size) {
                readCount.addAndGet(size);
            }
        };

        // 2-byte buffer ensures that the reader thread doesn't read too far ahead
        ParallelInputStream pStream = new ParallelInputStream(new ByteArrayInputStream(content), 2, readListener);

        // read 10 bytes
        byte[] buffer = new byte[10];
        int read, total = 0;
        do {
            read = pStream.read(buffer, total, buffer.length - total);
            System.out.println("read " + read + " bytes");
            if (read > 0) total += read;
        } while (read != -1 && total < 10);

        // close stream early as though a write error occurred in target storage
        pStream.close();

        Assertions.assertEquals(10, total);
        Assertions.assertArrayEquals(Arrays.copyOfRange(content, 0, 10), buffer);

        Assertions.assertFalse(pStream.reader.isComplete());
        Assertions.assertTrue(pStream.reader.isFailed());
        Assertions.assertFalse(pStream.readerThread.isAlive());
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

    private class FailingInputStream extends FilterInputStream {
        private char failChar;
        private String errorMessage;
        private IOException error;

        public FailingInputStream(InputStream in, char failChar, String errorMessage) {
            super(in);
            this.failChar = failChar;
            this.errorMessage = errorMessage;
        }

        @Override
        public int read() throws IOException {
            int result = super.read();
            if ((char) result == failChar) throw new IOException(errorMessage);
            return result;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        // read all bytes prior to fail character, then throw exception on next read; this is for testing purposes
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (error != null) throw error;
            int i = 0;
            try {
                for (; i < len; i++) {
                    int c = read();
                    if (c == -1 || error != null) break;
                    b[off + i] = (byte) c;
                }
            } catch (IOException e) {
                error = e;
            }
            return i == 0 ? -1 : i;
        }
    }
}

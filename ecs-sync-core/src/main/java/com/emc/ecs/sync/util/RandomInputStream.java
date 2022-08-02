/*
 * Copyright (c) 2015-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

public class RandomInputStream extends InputStream {
    Random random = new Random();
    private long size;
    private boolean closed = false;

    public RandomInputStream(long size) {
        this.size = size;
    }

    @Override
    public int read() throws IOException {
        if (closed) throw new IOException("stream closed");
        if (size <= 0) return -1;
        size--;
        return random.nextInt(256); // 0 <= value < 256
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (size <= 0) return -1;
        if (len > size) len = (int) size;
        for (int i = 0; i < len; )
            for (int rnd = random.nextInt(), n = Math.min(len - i, 4); n-- > 0; rnd >>= 8)
                b[off + i++] = (byte) rnd;
        size -= len;
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n < 0) throw new IllegalArgumentException("argument must be positive");
        if (n > size) n = size;
        size -= n; // 0 <= n <= size
        return n;
    }

    @Override
    public int available() throws IOException {
        return (int) Math.min((long) Integer.MAX_VALUE, size);
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }
}

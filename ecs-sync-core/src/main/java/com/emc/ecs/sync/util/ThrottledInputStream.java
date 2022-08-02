/*
 * Copyright (c) 2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import engineering.clientside.throttle.Throttle;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Throttles the read rate based on supplied <code>Throttle</code> instances.
 * Permits are acquired from all supplied throttles <em>after</em> each buffer is read to ensure the actual amount read
 * is consumed, since it may be smaller than the amount requested.
 */
public class ThrottledInputStream extends FilterInputStream {
    private final List<Throttle> throttles;

    public ThrottledInputStream(InputStream inputStream, Throttle... throttles) {
        super(inputStream);
        // remove null values
        this.throttles = Arrays.stream(throttles).filter(Objects::nonNull).collect(Collectors.toList());
    }

    /**
     * Unsupported because Single Byte read is extremely slow to enable throttle.
     */
    @Override
    public int read() throws IOException {
        // extremely high overhead is expected to support throttle here.
        throw new UnsupportedOperationException("Single Byte read is extremely slow to enable throttle.");
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        return this.read(bytes, 0, bytes.length);
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        int n = super.read(bytes, off, len);
        applyThrottle(n);
        return n;
    }

    // TODO: depending on the origin stream, skip() may actually end up reading l bytes of data, in which case, this
    //       method should throttle that read.. need to find a way to determine if that is necessary
    @Override
    public long skip(long l) throws IOException {
        return super.skip(l);
    }

    protected void applyThrottle(int bytes) throws IOException {
        if (bytes > 0 && throttles != null) {
            try {
                // must apply all throttles
                // to eliminate overhead, we will acquire from all throttles asynchronously, and sleep for whichever delay is longer
                long maxWaitTime = throttles.stream().mapToLong(
                        throttle -> throttle.acquireDelayDuration(bytes)
                ).max().orElse(0);
                if (maxWaitTime > 0) NANOSECONDS.sleep(maxWaitTime);
            } catch (InterruptedException e) {
                throw new IOException("Interrupted during throttle wait", e);
            }
        }
    }
}

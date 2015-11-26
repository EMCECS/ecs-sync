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

import com.filepool.fplibrary.FPStreamInterface;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CasInputStream extends CountingInputStream implements FPStreamInterface {
    public static final int MAX_BUFFER = 1048576;

    protected static MessageDigest createMd5Digest() {
        try {
            return MessageDigest.getInstance("md5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not initialize MD5", e);
        }
    }

    private long size;
    private boolean closed = false;

    public CasInputStream(InputStream in, long size) {
        this(in, size, false);
    }

    public CasInputStream(InputStream in, long size, boolean calculateMd5) {
        super(calculateMd5 ? new DigestInputStream(in, createMd5Digest()) : in);
        this.size = size;
    }

    @Override
    public void close() throws IOException {
        super.close();
        closed = true;
    }

    @Override
    public long getStreamLength() {
        return size;
    }

    @Override
    public boolean FPMarkSupported() {
        return super.markSupported();
    }

    @Override
    public void FPMark() {
        super.mark(MAX_BUFFER);
    }

    @Override
    public void FPReset() throws IOException {
        super.reset();
    }

    public byte[] getMd5Digest() {
        if (!(in instanceof DigestInputStream)) throw new UnsupportedOperationException("MD5 checksum is not enabled");
        if (!closed) throw new UnsupportedOperationException("cannot get MD5 until stream is closed");
        return ((DigestInputStream) in).getMessageDigest().digest();
    }
}

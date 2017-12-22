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
package com.emc.ecs.sync.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * InputStream wrapper that counts the number of bytes that have been read and (optionally) calculates a checksum on the
 * data
 */
public class EnhancedInputStream extends FilterInputStream {
    public static final int UNSIZED = -1;

    protected static MessageDigest createMd5Digest() {
        try {
            return MessageDigest.getInstance("md5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not initialize MD5", e);
        }
    }

    private long size;
    private boolean closed = false;
    private long bytesRead = 0;

    public EnhancedInputStream(InputStream in) {
        this(in, UNSIZED, false);
    }

    public EnhancedInputStream(InputStream in, long size) {
        this(in, size, false);
    }

    public EnhancedInputStream(InputStream in, boolean calculateMd5) {
        this(in, UNSIZED, calculateMd5);
    }

    public EnhancedInputStream(InputStream in, long size, boolean calculateMd5) {
        super(calculateMd5 ? new DigestInputStream(in, createMd5Digest()) : in);
        this.size = size;
    }

	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int c = super.read(b, off, len);
		if(c != -1) {
			bytesRead += c;
		}
		return c;
	}
	
	@Override
	public int read() throws IOException {
		int v = super.read();
		if (v != -1) bytesRead++;
		return v;
	}

	@Override
	public void close() throws IOException {
		super.close();
		closed = true;
	}

    public long getSize() {
        return size;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * @return the total number of bytes read
     */
    public long getBytesRead() {
		return bytesRead;
	}

    public byte[] getMd5Digest() {
        if (!(in instanceof DigestInputStream)) throw new UnsupportedOperationException("MD5 checksum is not enabled");
        if (!closed) throw new UnsupportedOperationException("cannot get MD5 until stream is closed");
        return ((DigestInputStream) in).getMessageDigest().digest();
    }
}

/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * InputStream wrapper that counts the number of bytes that have been read
 */
public class CountingInputStream extends FilterInputStream {
	private long bytesRead;
	private boolean closed = false;
	
	public CountingInputStream(InputStream in) {
		super(in);
		bytesRead = 0;
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

	/**
	 * @return the total number of bytes read
	 */
	public long getBytesRead() {
		return bytesRead;
	}

	public boolean isClosed() {
		return closed;
	}
}

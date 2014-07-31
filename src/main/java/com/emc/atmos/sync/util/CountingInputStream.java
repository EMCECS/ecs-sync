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
package com.emc.atmos.sync.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * InputStream wrapper that counts the number of bytes that have been read
 */
public class CountingInputStream extends InputStream {
	private InputStream in;
	private long bytesRead;
	
	public CountingInputStream(InputStream in) {
		this.in = in;
		bytesRead = 0;
	}

	@Override
	public int available() throws IOException {
		return in.available();
	}
	
	@Override
	public void close() throws IOException {
		in.close();
	}
	
	@Override
	public boolean equals(Object arg0) {
		return in.equals(arg0);
	}
	
	@Override
	public int hashCode() {
		return in.hashCode();
	}
	
	@Override
	public synchronized void mark(int readlimit) {
		in.mark(readlimit);
	}
	
	@Override
	public boolean markSupported() {
		return in.markSupported();
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		int c = in.read(b);
		if(c != -1) {
			bytesRead += c;
		}
		return c;
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int c = in.read(b, off, len);
		if(c != -1) {
			bytesRead += c;
		}
		return c;
	}
	
	@Override
	public synchronized void reset() throws IOException {
		in.reset();
	}
	
	@Override
	public long skip(long n) throws IOException {
		return in.skip(n);
	}
	
	@Override
	public String toString() {
		return in.toString();
	}
	
	/**
	 * @see java.io.InputStream#read()
	 */
	@Override
	public int read() throws IOException {
		int v = in.read();
		if(v != -1) {
			bytesRead++;
		}
		return v;
	}

	/**
	 * @return the total number of bytes read
	 */
	public long getBytesRead() {
		return bytesRead;
	}

}

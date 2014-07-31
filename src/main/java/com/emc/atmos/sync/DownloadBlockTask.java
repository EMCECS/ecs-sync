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
package com.emc.atmos.sync;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

import com.emc.esu.api.EsuApi;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.Extent;
import com.emc.esu.api.ObjectPath;

public class DownloadBlockTask extends TaskNode {
	private static final Logger l4j = Logger.getLogger(DownloadBlockTask.class);

	private FileChannel channel;
	private EsuApi esu;
	private Extent extent;
	private DownloadCompleteTask listener;
	private ObjectPath path;
	private boolean aborted = false;

	@Override
	protected TaskResult execute() throws Exception {
		if( aborted ) {
			// Abort, abort!
			l4j.debug( this + ": aborted" );
			return new TaskResult(false);
		}
		try {
			byte[] data = null;
			while(true) {
				try {
					data = esu.readObject(path, extent, null);
					break;
				} catch(EsuException e) {
					System.err.println( "Read failed: " + e + "( " + e.getCause() + "), retrying" );
					try {
						Thread.sleep(500);
					} catch (InterruptedException e1) {
						l4j.debug( "Sleep interrupted", e1 );
					}
				}
			}

			channel.write(ByteBuffer.wrap(data), extent.getOffset());

			listener.complete(this);
		} catch (IOException e) {
			listener.error(this, e);
			return new TaskResult(false);
		}
		return new TaskResult(true);
	}

	public void setChannel(FileChannel channel) {
		this.channel = channel;
	}

	public void setEsu(EsuApi esu) {
		this.esu = esu;
	}

	public void setExtent(Extent extent) {
		this.extent = extent;
	}

	public void setListener(DownloadCompleteTask dct) {
		this.listener = dct;
	}

	public void setPath(ObjectPath path) {
		this.path = path;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((extent == null) ? 0 : extent.hashCode());
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DownloadBlockTask other = (DownloadBlockTask) obj;
		if (extent == null) {
			if (other.extent != null)
				return false;
		} else if (!extent.equals(other.extent))
			return false;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "DownloadBlockTask [extent=" + extent + ", path=" + path + "]";
	}

	public void abort() {
		// TODO Auto-generated method stub
		
	}

	
}

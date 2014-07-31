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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.emc.esu.api.ObjectPath;

public class DownloadCompleteTask extends TaskNode {
	private static final Logger l4j = Logger.getLogger( DownloadCompleteTask.class );

	private FileChannel channel;
	private Set<DownloadBlockTask> blocks;
	private Set<DownloadBlockTask> completedBlocks;

	private ObjectPath path;
	private File file;
	private boolean successful = true;
	private Exception failure;
	private AtmosSync atmosSync;
	private long size;

	private Date mtime;
	
	public DownloadCompleteTask() {
		completedBlocks = Collections.synchronizedSet( 
				new HashSet<DownloadBlockTask>() );
	}


	@Override
	protected TaskResult execute() throws Exception {
		channel.close();
		
		if( successful ) {
			file.setLastModified( mtime.getTime() );
			atmosSync.success(this, file, path, size);
			return new TaskResult(true);
		} else {
			atmosSync.failure(this, file, path, failure);
			return new TaskResult(false);
		}
	}


	public void error(DownloadBlockTask downloadBlockTask, IOException e) {
		successful = false;
		failure = e;
		
		// Cancel the rest of the tasks
		for( DownloadBlockTask bt : blocks ) {
			bt.abort();
		}
	}

	public void complete(DownloadBlockTask downloadBlockTask) {
		completedBlocks.add(downloadBlockTask);
		
		l4j.debug( "Complete: " + downloadBlockTask );
	}


	public void setChannel(FileChannel channel) {
		this.channel = channel;
	}


	public void setBlocks(Set<DownloadBlockTask> blocks) {
		this.blocks = blocks;
	}


	public void setAtmosSync(AtmosSync atmosSync) {
		this.atmosSync = atmosSync;
	}


	public void setPath(ObjectPath path) {
		this.path = path;
	}


	public void setFile(File file) {
		this.file = file;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		DownloadCompleteTask other = (DownloadCompleteTask) obj;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		return true;
	}


	@Override
	public String toString() {
		return "DownloadCompleteTask [path=" + path + ", file=" + file + "]";
	}


	public void setSize(long filesize) {
		this.size = filesize;
	}


	public void setMtime(Date remoteMtime) {
		this.mtime = remoteMtime;
	}

}

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
import java.util.List;

import org.apache.log4j.Logger;

import com.emc.atmos.sync.AtmosSync.METADATA_MODE;
import com.emc.esu.api.DirectoryEntry;
import com.emc.esu.api.ListOptions;
import com.emc.esu.api.ObjectPath;

public class SyncTask extends TaskNode {
	public static final Logger l4j = Logger.getLogger(SyncTask.class);

	private File localDir;
	private ObjectPath remoteDir;
	private AtmosSync sync;
	
	public SyncTask( File localDir, ObjectPath remoteDir, AtmosSync sync ) {
		super( null );
		this.localDir = localDir;
		this.remoteDir = remoteDir;
		this.sync = sync;
	}

	
	@Override
	protected TaskResult execute() throws Exception {
		try {
			switch( sync.getSyncMode() ) {
			case UPLOAD_ONLY:
				doUpload();
				break;
			case DOWNLOAD_ONLY:
				doDownload();
				break;
			}
		} catch( Throwable t ) {
			l4j.error( "Error synchronizing " + localDir + "->" + remoteDir + ": " + t, t );
			return new TaskResult(false);
		}
		
		return new TaskResult( true );
	}
	
	private void doDownload() {
		l4j.debug( "Synchronizing " + remoteDir + " to " + localDir );
		if( !localDir.exists() ) {
			if( !localDir.mkdirs() ) {
				throw new RuntimeException( "Could not create local directory " + localDir );
			}
		}
		
		ListOptions options = new ListOptions();
		options.setIncludeMetadata(true);
		List<DirectoryEntry> ents = sync.getEsu().listDirectory( remoteDir, options );
		downloadEntries( ents );
		while( options.getToken() != null ) {
			// Continue
			ents = sync.getEsu().listDirectory( remoteDir, options );
			downloadEntries( ents );
		}
	}


	private void downloadEntries(List<DirectoryEntry> ents) {
		for( DirectoryEntry ent : ents ) {
			// Special case: don't recurse down into the listable tags
			// folder.
			if( "/apache/".equals( ent.getPath().toString() ) ) {
				l4j.debug( "Skipping " + ent.getPath() );
				continue;
			}
			
			if( "directory".equals( ent.getType() ) ) {
				SyncTask task = new SyncTask(  
						new File( localDir, ent.getPath().getName() ), 
						ent.getPath(), sync );
				task.addParent( this );
				task.addToGraph( sync.getGraph() );
			} else {
				AtmosDownloadTask download = new AtmosDownloadTask( 
						new File( localDir, ent.getPath().getName() ), 
						ent, sync );
				sync.incrementFileCount();
				download.addToGraph( sync.getGraph() );
				
				if(sync.isSyncingMetadata()) {
					AtmosDownloadMetaTask mdownload = new AtmosDownloadMetaTask(
							ent.getPath(),
							new File(new File(localDir, AtmosSync.META_DIR), ent.getPath().getName()),
							sync );
					mdownload.addToGraph( sync.getGraph() );
				}
			}
		}
	}


	private void doUpload() {
		l4j.debug( "Synchronizing " + localDir + " to " + remoteDir );
		TaskNode mkdirTask = null;
		if( sync.getAcl() != null 
				|| sync.getMetadataMode() == METADATA_MODE.BOTH 
				|| sync.getMetadataMode() == METADATA_MODE.DIRECTORIES ) {
			mkdirTask = new AtmosMkdirTask( remoteDir, sync );
			mkdirTask.addToGraph( sync.getGraph() );
		}
		
		File[] files = localDir.listFiles();
		for( File f : files ) {
			if( f.isDirectory() ) {
				SyncTask subDirSync = new SyncTask( f, new ObjectPath( remoteDir + f.getName() + "/" ), sync );
				if( mkdirTask != null ) {
					subDirSync.addParent( mkdirTask );
				}
				subDirSync.addParent( this );
				subDirSync.addToGraph( sync.getGraph() );
			} else if( f.isFile() ) {
				String mimeType = sync.getMimeMap().getContentType(f);
				String objectPath = AtmosSync.encodeObjectPath( remoteDir + f.getName() );
				AtmosUploadTask upload = new AtmosUploadTask( f, new ObjectPath( objectPath ), mimeType, sync );
				if( mkdirTask != null ) {
					upload.addParent( mkdirTask );
				}
				upload.addToGraph( sync.getGraph() );
				sync.incrementFileCount();
			}
		}
		
		if( files.length == 0 && sync.isDelete() ) {
			localDir.delete();
		}

	}


	@Override
	public int hashCode() {
		return localDir.hashCode();
	}


	@Override
	public boolean equals(Object obj) {
		if( !(obj instanceof SyncTask) ) {
			return false;
		}
		SyncTask other = (SyncTask)obj;
		return other.localDir.equals( this.localDir ) && other.remoteDir.equals( this.remoteDir );
	}


	@Override
	public String toString() {
		return "Synchronize " + localDir + " <-> " + remoteDir;
	}

}

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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

import com.emc.atmos.sync.AtmosSync.METADATA_MODE;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.Metadata;
import com.emc.esu.api.MetadataList;
import com.emc.esu.api.ObjectMetadata;
import com.emc.esu.api.ObjectPath;

public class AtmosUploadTask extends TaskNode {
	private static final Logger l4j = Logger.getLogger( AtmosUploadTask.class );

	private File file;
	private ObjectPath objectPath;
	private String mimeType;
	private AtmosSync atmosSync;

	public AtmosUploadTask(File file, ObjectPath objectPath, String mimeType,
			AtmosSync sync) {
		
		this.file = file;
		this.objectPath = objectPath;
		this.mimeType = mimeType;
		this.atmosSync = sync;
	}

	@Override
	protected TaskResult execute() throws Exception {
		// Check to see if the local file exists and/or matches the local
		boolean exists = false;
		ObjectMetadata om = null;
		try {
			om = atmosSync.getEsu().getAllMetadata(objectPath);
			exists = true;
		} catch( EsuException e ) {
			if( e.getHttpCode() == 404 ) {
				// Doesn't exist
				l4j.debug( "remote object " + objectPath + " doesn't exist" );
			} else {
				atmosSync.failure( this, file, objectPath, e );
				return new TaskResult(false);
			}
		} catch( Exception e ) {
			atmosSync.failure( this, file, objectPath, e );
			return new TaskResult(false);
		}
		
		if( exists && !atmosSync.isForce() ) {
			// Check size and atmossync_mtime
			long remoteSize = Long.parseLong(om.getMetadata().getMetadata( "size" ).getValue() );
			if( remoteSize != file.length() ) {
				l4j.debug( file + " size (" + file.length() + ") differs from remote " + objectPath + "(" + remoteSize + ")" );
			} else {
				// Check mtime
				if( om.getMetadata().getMetadata( AtmosSync.MTIME_NAME ) != null ) {
					long remoteMtime = Long.parseLong(om.getMetadata().getMetadata( AtmosSync.MTIME_NAME ).getValue() );
					if( remoteMtime != file.lastModified() ) {
						l4j.debug( file + " timestamp (" + file.lastModified() + ") differs from remote " + objectPath + "(" + remoteMtime + ")" );
					} else {
						// Matches
						l4j.debug( file + " matches remote " + objectPath );
						
						if( atmosSync.isDelete() ) {
							cleanup( file );
						}

						atmosSync.success( this, file, objectPath, 0 );
						return new TaskResult(true);
					}
				}
			}
		}
		
		// If we're here, it's time to copy!
		l4j.debug( "Uploading " + file + " to " + objectPath + " (" + mimeType + "): " + file.length() + " bytes" );
		
		// If ACLs are required, do a quick check to make sure the directory
		// exists.  There could be a slight delay sometimes between hosts.
		if( atmosSync.getAcl() != null ) {
			boolean dirExists = false;
			ObjectPath parentDir = AtmosSync.getParentDir( objectPath );
			
			dirExists = pathExists( parentDir );
			
			if( !dirExists ) {
				l4j.info( "Sleeping 100ms to wait for parent" );
				Thread.sleep(100);
				
				dirExists = pathExists( parentDir );
				if( !dirExists ) {
					throw new RuntimeException( "Parent directory " + parentDir + " does not exist!" );
				}
			}
			

		}
		
		InputStream in = null;
		try {
			in = new FileInputStream( file );
					
			MetadataList mlist = new MetadataList();
			mlist.addMetadata( new Metadata(AtmosSync.MTIME_NAME, ""+file.lastModified(), false ) );
			
			if( atmosSync.getMeta() != null 
					&& (atmosSync.getMetadataMode() == METADATA_MODE.BOTH 
					|| atmosSync.getMetadataMode() == METADATA_MODE.FILES ) ) {
				for( Metadata m : atmosSync.getMeta() ) {
					mlist.addMetadata( m );
				}
			}
			
			if( exists ) {
				atmosSync.getEsu().updateObjectFromStream(objectPath, atmosSync.getAcl(), mlist, null, in, file.length(), mimeType);
			} else {
				atmosSync.getEsu().createObjectFromStreamOnPath(objectPath, atmosSync.getAcl(), mlist, in, file.length(), mimeType);
			}
			
			if( atmosSync.isDelete() ) {
				cleanup( file );
			}

			atmosSync.success(this, file, objectPath, file.length());
			return new TaskResult( true );
		} catch (FileNotFoundException e) {
			atmosSync.failure(this, file, objectPath, e);
			return new TaskResult(false);
		} catch (EsuException e ) {
			atmosSync.failure(this, file, objectPath, e);
			return new TaskResult(false);
		} catch (Throwable t ) {
			atmosSync.failure(this, file, objectPath, new RuntimeException(t.toString(),t));
			return new TaskResult(false);
		} finally {
			if( in != null ) {
				try {
					in.close();
				} catch (IOException e) {
					// Ignore
				}
			}
		}
		

	}
	
	private boolean pathExists( ObjectPath path ) {
		try {
			atmosSync.getEsu().getAllMetadata( path );
			return true;
		} catch( EsuException e ) {
			if( e.getHttpCode() == 404 ) {
				// Doesn't exist
				l4j.debug( "remote object " + path + " doesn't exist" );
			} else {
				l4j.error( "get dir failed for " + path + ": " + e , e );
			}
		} catch( Exception e ) {
			l4j.error( "get dir failed for " + path + ": " + e , e );
		}
		
		return false;
	}
	
	@Override
	public boolean equals(Object obj) {
		if( !(obj instanceof AtmosUploadTask) ) {
			return false;
		}
		AtmosUploadTask other = (AtmosUploadTask)obj;
		return other.file.equals(file) && other.objectPath.equals(objectPath);
	}

	@Override
	public int hashCode() {
		return file.hashCode();
	}

	@Override
	public String toString() {
		return file.toString() + " -> " + objectPath.toString();
	}
	
	private void cleanup(File file) {
		if( file.equals( atmosSync.getLocalroot() ) ) {
			// Stop here
			return;
		}
		l4j.info( "Deleting " + file );
		if( !file.delete() ) {
			l4j.warn( "Failed to delete " + file );
			return;
		}
		
		// If it's a directory, see if it's empty.
		File parent = file.getParentFile();
		if( parent.isDirectory() && parent.listFiles().length == 0 ) {
			cleanup( parent );
		}
		
	}

}

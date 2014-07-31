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

import org.apache.log4j.Logger;

import com.emc.atmos.sync.AtmosSync.METADATA_MODE;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.ObjectPath;

public class AtmosMkdirTask extends TaskNode {
	private static final Logger l4j = Logger.getLogger(AtmosMkdirTask.class);
	
	private ObjectPath dirPath;
	private AtmosSync sync;
	
	public AtmosMkdirTask( ObjectPath dirPath, AtmosSync sync ) {
		this.dirPath = dirPath;
		this.sync = sync;
	}

	@Override
	protected TaskResult execute() throws Exception {
		boolean exists = false;
		
		try {
			sync.getEsu().getAllMetadata(dirPath);
			exists = true;
		} catch( EsuException e ) {
			if( e.getHttpCode() == 404 ) {
				// Doesn't exist
				l4j.debug( "remote object " + dirPath + " doesn't exist" );
			} else {
				l4j.error( "mkdirs failed for " + dirPath + ": " + e , e );
				return new TaskResult(false);
			}
		} catch( Exception e ) {
			l4j.error( "mkdirs failed for " + dirPath + ": " + e , e );
			return new TaskResult(false);
		}
		if( !exists ) {
			l4j.info( "mkdir: " + dirPath );
			if( sync.getMetadataMode() == METADATA_MODE.BOTH 
					|| sync.getMetadataMode() == METADATA_MODE.DIRECTORIES ) {
				//l4j.info( "Creating " + dirPath + " with metadata " + sync.getMeta() );
				sync.getEsu().createObjectOnPath(dirPath, sync.getAcl(), 
						sync.getMeta(), null, null);
			} else {
				sync.getEsu().createObjectOnPath(dirPath, sync.getAcl(), 
						null, null, null);				
			}
		}
		
		return new TaskResult(true);
	}

}

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
package com.emc.atmos.sync.plugins;

import com.emc.atmos.api.ObjectId;
import com.emc.atmos.api.ObjectPath;

/**
 * Annotates an object with its destination Atmos ID if it was written to
 * an Atmos system.  Can be used by other plugins for easy reference.
 * @author cwikj
 */
public class DestinationAtmosId implements ObjectAnnotation {
	private ObjectId id;
	private ObjectPath path;
	
	public DestinationAtmosId() {
	}
	
	public DestinationAtmosId(ObjectId id) {
		this.id = id;
	}
	
	public DestinationAtmosId(ObjectId id, ObjectPath path) {
		this.id = id;
		this.path = path;
	}
	
	public ObjectId getId() {
		return id;
	}
	public void setId(ObjectId id) {
		this.id = id;
	}
	public ObjectPath getPath() {
		return path;
	}
	public void setPath(ObjectPath path) {
		this.path = path;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DestinationAtmosId [id=" + id + ", path=" + path + "]";
	}
	
}

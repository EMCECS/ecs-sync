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

import com.emc.atmos.sync.util.AtmosMetadata;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public abstract class SyncObject {
	private static final Logger l4j = Logger.getLogger(SyncObject.class);
	
	private URI sourceURI;
	private URI destURI;
	private boolean directory;
	private Set<ObjectAnnotation> annotations;
	private long size;
	private AtmosMetadata metadata;
	
	public SyncObject() {
		annotations = new HashSet<ObjectAnnotation>();
		metadata = new AtmosMetadata();
	}
	
	public abstract InputStream getInputStream();
	
	public URI getSourceURI() {
		return sourceURI;
	}

	public void setSourceURI(URI sourceURI) {
		this.sourceURI = sourceURI;
	}

	public URI getDestURI() {
		return destURI;
	}

	public void setDestURI(URI destURI) {
		this.destURI = destURI;
	}

	public boolean isDirectory() {
		return directory;
	}

	public void setDirectory(boolean directory) {
		this.directory = directory;
	}
	
	public void addAnnotation(ObjectAnnotation annotation) {
		annotations.add(annotation);
	}
	
	public Set<ObjectAnnotation> getAnnotations() {
		return annotations;
	}
	
	public <T extends ObjectAnnotation> Set<T> getAnnotations(Class<T> clazz) {
		Set<T> subset = new HashSet<T>();
		for(ObjectAnnotation ann : annotations) {
			if(ann.getClass().isAssignableFrom(clazz)) {
                @SuppressWarnings("unchecked") T tann = (T) ann;
				subset.add(tann);
			}
		}
		return subset;
	}
	
	/**
	 * Similar to getAnnotations but it expects only one instance of the class.
	 * If not found, it returns null.
	 */
	public <T extends ObjectAnnotation> T getAnnotation(Class<T> clazz) {
		Set<T> subset = getAnnotations(clazz);
		if(subset.size() < 1) {
			return null;
		}
		if(subset.size() > 1) {
			l4j.warn("More than one instance of annotation " + clazz + " found!");
		}
		return subset.iterator().next();
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	/**
	 * Gets the relative path for the object.  If the destination is a
	 * namespace destination, this path will be used when computing the 
	 * absolute path in the destination, relative to the destination root.
	 */
	public abstract String getRelativePath();

	/**
	 * @return the atmosMetadata
	 */
	public AtmosMetadata getMetadata() {
		return metadata;
	}

	/**
	 * @param atmosMetadata the atmosMetadata to set
	 */
	public void setMetadata(AtmosMetadata atmosMetadata) {
		this.metadata = atmosMetadata;
	}

	public abstract long getBytesRead();
}

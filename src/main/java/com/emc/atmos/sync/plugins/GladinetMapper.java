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
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.api.bean.ObjectEntry;
import com.emc.atmos.api.request.ListObjectsRequest;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.*;

/**
 * Creates the necessary metadata mappings to upload data to Gladinet.  Note
 * that if you configure this plugin using Spring, you need to set the 
 * destination property to your AtmosDestination.
 * @author cwikj
 *
 */
public class GladinetMapper extends SyncPlugin {
	private static final String DIRECTORY_FLAG = "Directory";
	private static final String FILE_FLAG = "GCDFile";
	private static final Logger l4j = Logger.getLogger(GladinetMapper.class);
	private static final String GLADINET_ROOT = "GCDHOST"; // The root dir listable tag
	
	public static final String GLADINET_OPTION = "gladinet";
	public static final String GLADINET_DESC = "Activates the Gladinet metadata mapper plugin to write objects to Atmos in a manner compatible with Gladinet Cloud Desktop";
	
	public static final String GLADINET_DIR_OPTION = "gladinet-dir";
	public static final String GLADINET_DIR_DESC = "Sets the base directory in Gladinet to load content into.  This directory must already exist.";
	public static final String GLADINET_DIR_ARG_NAME = "base-directory";
	
	private static final String NAME_TAG = "GCDName";
	private static final String TYPE_TAG = "GCDTYPE";
	private static final String VERSION_TAG = "GCDVer";
	private static final String HOST_TAG = "GCDHOST";
	private static final String VERSION_VALUE = "2";
	
	private static final List<String> GLADINET_TAGS = 
			Collections.unmodifiableList(Arrays.asList(
					NAME_TAG,
					TYPE_TAG,
					VERSION_TAG,
					HOST_TAG));

	private AtmosDestination destination;
	private Random random;
	private String baseDir;
	
	/**
	 * Caches the mapping of directories to listable tags.
	 */
	private Map<String,String> dirCache;
	private Map<String,ObjectId> idCache;
	
	public GladinetMapper() {
		dirCache = Collections.synchronizedMap(
				new WeakHashMap<String, String>());
		idCache = Collections.synchronizedMap(
				new WeakHashMap<String, ObjectId>());
		random = new Random();
		baseDir = "";
	}
	
	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#filter(com.emc.atmos.sync.plugins.SyncObject)
	 */
	@Override
	public void filter(SyncObject obj) {
		String relativePath = obj.getRelativePath();
		if(relativePath.isEmpty()) {
			l4j.debug("Skipping root directory for Gladinet");
			return;
		}
		if(!baseDir.isEmpty()) {
			relativePath = baseDir + "/" + relativePath;
		}
		if(obj.isDirectory()) {
			String parentDir = getParentDir(relativePath);
			String parentTag = getTag(parentDir);
			String dirTag = getTag(relativePath);
			String dirName = getName(relativePath);
			if(dirTag == null) {
				// Generate a new tag
				dirTag = MessageFormat.format("{0}/{1}_GCD{2,number,000000}", 
						parentTag, dirName, random.nextInt(999999));
			} else {
				ObjectId dirId = getDirectoryId(parentTag, dirName);
				obj.addAnnotation(new DestinationAtmosId(dirId));
			}
			
			Map<String, Metadata> meta = obj.getMetadata().getMetadata();
			
			LogMF.debug(l4j, "Directory tag: {0}", dirTag);
			
			// Add the Gladinet tags
			meta.put( TYPE_TAG, new Metadata( TYPE_TAG, DIRECTORY_FLAG, false ) );
			meta.put( NAME_TAG, new Metadata( NAME_TAG, dirName, false ) );
			meta.put( VERSION_TAG, new Metadata( VERSION_TAG, VERSION_VALUE, false ) );
			if("".equals(parentDir)) {
				meta.put( HOST_TAG, new Metadata( HOST_TAG, dirTag, true ) );
			} else {
				meta.put( HOST_TAG, new Metadata( HOST_TAG, dirTag, false ) );
				// Sometimes, the name of the dir doesn't match up with the
				// last component of the host (usually 'New20Folder' from
				// windows for some reason).
				String tagName = getName(dirTag);
				meta.put( parentTag, new Metadata( parentTag, tagName, true ) );
			}
			
		} else {
			String dir = getParentDir(relativePath);
			String dirTag = getTag(dir);
			
			if(dirTag == null) {
				throw new RuntimeException("The Gladinet directory " + dir 
						+ " does not exist");
			}
			
			String name = getName(relativePath);
			LogMF.debug(l4j, "Directory tag: {0}", dirTag);
			ObjectId objId = getObjectId(relativePath, dirTag);
			obj.addAnnotation(new DestinationAtmosId(objId));
			
			// Add the Gladinet tags
            Map<String, Metadata> meta = obj.getMetadata().getMetadata();
			meta.put( NAME_TAG, new Metadata( NAME_TAG, name, false ) );
			meta.put( dirTag, new Metadata( dirTag, FILE_FLAG, true ) );
		}
		getNext().filter(obj);
	}

	private ObjectId getObjectId(String relativePath, String dirTag) {
		String name = getName(relativePath);
		ObjectId id = idCache.get(relativePath);
		if(id != null) {
			return id;
		}
		ListObjectsRequest request = new ListObjectsRequest();
        request.metadataName(dirTag).includeMetadata(true).setUserMetadataNames(GLADINET_TAGS);
		do {
			List<ObjectEntry> results =
					destination.getAtmos().listObjects(request).getEntries();
			for(ObjectEntry result : results) {
				Metadata nameMeta = result.getUserMetadataMap().get(NAME_TAG);
				if(nameMeta != null && name.equals(nameMeta.getValue())) {
					// Found
					idCache.put(relativePath, result.getObjectId());
					return result.getObjectId();
				}
			}
		} while(request.getToken() != null);
		
		// Not found
		return null;
	}

	private ObjectId getDirectoryId(String parentTag, String dirName) {
		ObjectId id = idCache.get(parentTag + "/" + dirName);
		if(id != null) {
			return id;
		}
        ListObjectsRequest request = new ListObjectsRequest();
        request.metadataName(parentTag).includeMetadata(true).setUserMetadataNames(GLADINET_TAGS);
		do {
			List<ObjectEntry> results =
					destination.getAtmos().listObjects(request).getEntries();
			for(ObjectEntry result : results) {
				Metadata nameMeta = result.getUserMetadataMap().get(NAME_TAG);
				Metadata typeMeta = result.getUserMetadataMap().get(TYPE_TAG);
				if(nameMeta != null && typeMeta != null &&
						DIRECTORY_FLAG.equals(typeMeta.getValue()) &&
						dirName.equals(nameMeta.getValue())) {
					// Found
					idCache.put(parentTag + "/" + dirName, result.getObjectId());
					return result.getObjectId();
				}
			}
		} while(request.getToken() != null);
		
		// Not found
		return null;
	}

	private String getParentDir(String relativePath) {
		if(relativePath.length()>0 && relativePath.endsWith("/")) {
			// Dirs end with a slash.  Ignore this
			relativePath = relativePath.substring(0, relativePath.length()-1);
		}
		int lastslash = relativePath.lastIndexOf('/');
		if(lastslash == -1) {
			// No slashes.  This is the top dir.
			return "";
		}
		if(lastslash == 0) {
			// This shouldn't happen.
			throw new IllegalArgumentException(
					"Relative paths should not start with a slash: " + relativePath);
		}
		return(relativePath.substring(0, lastslash));
	}

	private synchronized String getTag(String relativePath) {
		if(relativePath.isEmpty()) {
			return GLADINET_ROOT;				
		}
		
		String tag = dirCache.get(relativePath);
		if(tag != null) {
			return tag;
		}
		
		// Lookup the tag.
		return lookupTag(relativePath);
	}

	private String lookupTag(String path) {
		String name = getName(path);
		String parent = getParentDir(path);
		
		String parentTag = getTag(parent);
		if(parentTag == null) {
			throw new RuntimeException("The Gladinet directory " + parent 
					+ " does not exist");
		}

        ListObjectsRequest request = new ListObjectsRequest();
        request.metadataName(parentTag).includeMetadata(true).setUserMetadataNames(GLADINET_TAGS);
		do {
			List<ObjectEntry> results = destination.getAtmos().listObjects(request).getEntries();
			for(ObjectEntry r : results) {
				if(r.getUserMetadataMap().get(NAME_TAG) != null) {
					if(name.equals(r.getUserMetadataMap().get(NAME_TAG).getValue())) {
						// Found.
						String childTag;
						String host = r.getUserMetadataMap().get(HOST_TAG).getValue();
						if(GLADINET_ROOT.equals(parentTag)) {
							childTag = GLADINET_ROOT + "/" + host;
						} else {
							childTag = host;
						}
						dirCache.put(path, childTag);
						idCache.put(path, r.getObjectId());
						return childTag;
					}
				}
			}
		} while(request.getToken() != null);
		
		// Not found.
		return null;
	}

	private String getName(String path) {
		if(path.length()>0 && path.endsWith("/")) {
			// Dirs end with a slash.  Ignore this
			path = path.substring(0, path.length()-1);
		}
		
		int lastslash = path.lastIndexOf('/');
		if(lastslash == -1) {
			// It's a root object
			return path;
		}
		if(lastslash == 0) {
			throw new IllegalArgumentException(
					"Relative paths should not start with a slash: " + path);
		}
		return path.substring(lastslash+1);

	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options opts = new Options();
		
		opts.addOption(OptionBuilder.withDescription(GLADINET_DESC)
				.withLongOpt(GLADINET_OPTION).create());
		
		opts.addOption(OptionBuilder.withDescription(GLADINET_DIR_DESC)
				.withLongOpt(GLADINET_DIR_OPTION)
				.hasArg().withArgName(GLADINET_DIR_ARG_NAME).create());
		
		return opts;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons.cli.CommandLine)
	 */
	@Override
	public boolean parseOptions(CommandLine line) {
		if(line.hasOption(GLADINET_OPTION)) {
			if(line.hasOption(GLADINET_DIR_OPTION)) {
				baseDir = line.getOptionValue(GLADINET_DIR_OPTION);
				if(baseDir.contains("\\")) {
					baseDir = baseDir.replace('\\', '/');
				}
				
				// Normalize it so it doesn't contain leading or trailing
				// slashes
				if(baseDir.startsWith("/")) {
					baseDir = baseDir.substring(1);
				}
				if(baseDir.endsWith("/")) {
					baseDir = baseDir.substring(0, baseDir.length()-1);
				}
			}
			return true;
		}

		return false;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#validateChain(com.emc.atmos.sync.plugins.SyncPlugin)
	 */
	@Override
	public void validateChain(SyncPlugin first) {
		// The destination must be an AtmosDestination plugin configured in
		// object space mode.
		SyncPlugin last = getNext();
		while(last.getNext() != null) {
			last = last.getNext();
		}
		
		if(!(last instanceof AtmosDestination)) {
			throw new RuntimeException("The " + getName() + " plugin is only compatible with Atmos Destinations");
		}
		destination = (AtmosDestination)last;
		if(destination.getDestNamespace() != null) {
			throw new RuntimeException("When using the " + getName() + " plugin, the Atmos Destination must be in object mode, not namespace mode.");
		}
		
		// If a root directory was specified, make sure it exists
		if(!baseDir.isEmpty()) {
			String tag = getTag(baseDir);
			if(tag == null) {
				throw new RuntimeException("The Gladinet base directory " 
						+ baseDir + " does not exist");
			}
		}
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getName()
	 */
	@Override
	public String getName() {
		return "Gladinet Mapper";
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
	 */
	@Override
	public String getDocumentation() {
		return "This plugin creates the appropriate metadata in Atmos to " +
				"upload data in a fashion compatible with Gladinet's Cloud " +
				"Desktop software when it's hosted by EMC Atmos.";
	}

}

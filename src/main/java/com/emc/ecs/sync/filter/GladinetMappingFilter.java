/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.filter;

import com.emc.atmos.api.AtmosApi;
import com.emc.atmos.api.ObjectId;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.api.bean.ObjectEntry;
import com.emc.atmos.api.request.ListObjectsRequest;
import com.emc.ecs.sync.config.filter.GladinetMappingConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.config.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;

import static com.emc.ecs.sync.model.ObjectMetadata.UserMetadata;

/**
 * Creates the necessary metadata mappings to upload data to Gladinet.  Note
 * that if you configure this plugin using Spring, you need to set the
 * target property to your AtmosTarget.
 *
 * @author cwikj
 */
public class GladinetMappingFilter extends AbstractFilter<GladinetMappingConfig> {
    private static final Logger log = LoggerFactory.getLogger(GladinetMappingFilter.class);

    private static final String DIRECTORY_FLAG = "Directory";
    private static final String FILE_FLAG = "GCDFile";
    private static final String GLADINET_ROOT = "GCDHOST"; // The root dir listable tag

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

    private AtmosApi targetAtmos;
    private Random random;
    private String baseDir;

    /**
     * Caches the mapping of directories to listable tags.
     */
    private Map<String, String> dirCache;
    private Map<String, ObjectId> idCache;

    public GladinetMappingFilter() {
        dirCache = Collections.synchronizedMap(
                new WeakHashMap<String, String>());
        idCache = Collections.synchronizedMap(
                new WeakHashMap<String, ObjectId>());
        random = new Random();
        baseDir = "";
    }

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        // The target must be an AtmosTarget plugin configured in
        // object space mode.
        // TODO: write AtmosStorage
//        if (!(target instanceof AtmosStorage)) {
//            throw new ConfigurationException("This plugin is only compatible with Atmos Targets");
//        }
//        if (((AtmosStorage) target).getConfig().getDestNamespace() != null) {
//            throw new ConfigurationException("When using the Gladinet plugin, the Atmos Target must be in object mode, not namespace mode.");
//        }
//        this.targetAtmos = ((AtmosStorage) target).getAtmos();

        if (baseDir == null) baseDir = "";

        baseDir = baseDir.replace('\\', '/');

        // Normalize it so it doesn't contain leading or trailing
        // slashes
        if (baseDir.startsWith("/")) {
            baseDir = baseDir.substring(1);
        }
        if (baseDir.endsWith("/")) {
            baseDir = baseDir.substring(0, baseDir.length() - 1);
        }

        // If a root directory was specified, make sure it exists
        if (!baseDir.isEmpty()) {
            String tag = getTag(baseDir);
            if (tag == null) {
                throw new ConfigurationException("The Gladinet base directory " + baseDir + " does not exist");
            }
        }
    }

    @Override
    public void filter(ObjectContext objectContext) {
        SyncObject obj = objectContext.getObject();
        String relativePath = obj.getRelativePath();
        if (relativePath.isEmpty()) {
            log.debug("Skipping root directory for Gladinet");
            return;
        }
        if (!baseDir.isEmpty()) {
            relativePath = baseDir + "/" + relativePath;
        }

        Map<String, UserMetadata> meta = obj.getMetadata().getUserMetadata();

        if (obj.getMetadata().isDirectory()) {
            String parentDir = getParentDir(relativePath);
            String parentTag = getTag(parentDir);
            String dirTag = getTag(relativePath);
            String dirName = getName(relativePath);
            if (dirTag == null) {
                // Generate a new tag
                dirTag = MessageFormat.format("{0}/{1}_GCD{2,number,000000}",
                        parentTag, dirName, random.nextInt(999999));
            } else {
                ObjectId dirId = getDirectoryId(parentTag, dirName);
                if (dirId != null) objectContext.setTargetId(dirId.toString());
            }

            log.debug("Directory tag: {}", dirTag);

            // Add the Gladinet tags
            meta.put(TYPE_TAG, new UserMetadata(TYPE_TAG, DIRECTORY_FLAG, false));
            meta.put(NAME_TAG, new UserMetadata(NAME_TAG, dirName, false));
            meta.put(VERSION_TAG, new UserMetadata(VERSION_TAG, VERSION_VALUE, false));
            if ("".equals(parentDir)) {
                meta.put(HOST_TAG, new UserMetadata(HOST_TAG, dirTag, true));
            } else {
                meta.put(HOST_TAG, new UserMetadata(HOST_TAG, dirTag, false));
                // Sometimes, the name of the dir doesn't match up with the
                // last component of the host (usually 'New20Folder' from
                // windows for some reason).
                String tagName = getName(dirTag);
                meta.put(parentTag, new UserMetadata(parentTag, tagName, true));
            }

        } else {
            String dir = getParentDir(relativePath);
            String dirTag = getTag(dir);

            if (dirTag == null) {
                throw new RuntimeException("The Gladinet directory " + dir
                        + " does not exist");
            }

            String name = getName(relativePath);
            log.debug("Directory tag: {}", dirTag);
            ObjectId objId = getObjectId(relativePath, dirTag);
            if (objId != null) objectContext.setTargetId(objId.toString());

            // Add the Gladinet tags
            meta.put(NAME_TAG, new UserMetadata(NAME_TAG, name, false));
            meta.put(dirTag, new UserMetadata(dirTag, FILE_FLAG, true));
        }
        getNext().filter(objectContext);
    }

    // TODO: if verification ever includes metadata, revert metadata changes here
    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }

    private ObjectId getObjectId(String relativePath, String dirTag) {
        String name = getName(relativePath);
        ObjectId id = idCache.get(relativePath);
        if (id != null) {
            return id;
        }
        ListObjectsRequest request = new ListObjectsRequest();
        request.metadataName(dirTag).includeMetadata(true).setUserMetadataNames(GLADINET_TAGS);
        do {
            List<ObjectEntry> results = targetAtmos.listObjects(request).getEntries();
            for (ObjectEntry result : results) {
                Metadata nameMeta = result.getUserMetadataMap().get(NAME_TAG);
                if (nameMeta != null && name.equals(nameMeta.getValue())) {
                    // Found
                    idCache.put(relativePath, result.getObjectId());
                    return result.getObjectId();
                }
            }
        } while (request.getToken() != null);

        // Not found
        return null;
    }

    private ObjectId getDirectoryId(String parentTag, String dirName) {
        ObjectId id = idCache.get(parentTag + "/" + dirName);
        if (id != null) {
            return id;
        }
        ListObjectsRequest request = new ListObjectsRequest();
        request.metadataName(parentTag).includeMetadata(true).setUserMetadataNames(GLADINET_TAGS);
        do {
            List<ObjectEntry> results = targetAtmos.listObjects(request).getEntries();
            for (ObjectEntry result : results) {
                Metadata nameMeta = result.getUserMetadataMap().get(NAME_TAG);
                Metadata typeMeta = result.getUserMetadataMap().get(TYPE_TAG);
                if (nameMeta != null && typeMeta != null &&
                        DIRECTORY_FLAG.equals(typeMeta.getValue()) &&
                        dirName.equals(nameMeta.getValue())) {
                    // Found
                    idCache.put(parentTag + "/" + dirName, result.getObjectId());
                    return result.getObjectId();
                }
            }
        } while (request.getToken() != null);

        // Not found
        return null;
    }

    private String getParentDir(String relativePath) {
        if (relativePath.length() > 0 && relativePath.endsWith("/")) {
            // Dirs end with a slash.  Ignore this
            relativePath = relativePath.substring(0, relativePath.length() - 1);
        }
        int lastslash = relativePath.lastIndexOf('/');
        if (lastslash == -1) {
            // No slashes.  This is the top dir.
            return "";
        }
        if (lastslash == 0) {
            // This shouldn't happen.
            throw new IllegalArgumentException(
                    "Relative paths should not start with a slash: " + relativePath);
        }
        return (relativePath.substring(0, lastslash));
    }

    private synchronized String getTag(String relativePath) {
        if (relativePath.isEmpty()) {
            return GLADINET_ROOT;
        }

        String tag = dirCache.get(relativePath);
        if (tag != null) {
            return tag;
        }

        // Lookup the tag.
        return lookupTag(relativePath);
    }

    private String lookupTag(String path) {
        String name = getName(path);
        String parent = getParentDir(path);

        String parentTag = getTag(parent);
        if (parentTag == null) {
            throw new RuntimeException("The Gladinet directory " + parent
                    + " does not exist");
        }

        ListObjectsRequest request = new ListObjectsRequest();
        request.metadataName(parentTag).includeMetadata(true).setUserMetadataNames(GLADINET_TAGS);
        do {
            List<ObjectEntry> results = targetAtmos.listObjects(request).getEntries();
            for (ObjectEntry r : results) {
                if (r.getUserMetadataMap().get(NAME_TAG) != null) {
                    if (name.equals(r.getUserMetadataMap().get(NAME_TAG).getValue())) {
                        // Found.
                        String childTag;
                        String host = r.getUserMetadataMap().get(HOST_TAG).getValue();
                        if (GLADINET_ROOT.equals(parentTag)) {
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
        } while (request.getToken() != null);

        // Not found.
        return null;
    }

    private String getName(String path) {
        if (path.length() > 0 && path.endsWith("/")) {
            // Dirs end with a slash.  Ignore this
            path = path.substring(0, path.length() - 1);
        }

        int lastslash = path.lastIndexOf('/');
        if (lastslash == -1) {
            // It's a root object
            return path;
        }
        if (lastslash == 0) {
            throw new IllegalArgumentException(
                    "Relative paths should not start with a slash: " + path);
        }
        return path.substring(lastslash + 1);

    }
}

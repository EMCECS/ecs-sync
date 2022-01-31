/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.storage.s3;

import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.AbstractStorage;
import com.emc.ecs.sync.storage.ObjectNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public abstract class AbstractS3Storage<C> extends AbstractStorage<C> {
    private static final Logger log = LoggerFactory.getLogger(AbstractS3Storage.class);

    static final String PROP_OBJECT_VERSIONS = "s3.objectVersions";
    static final String PROP_IS_NEW_OBJECT = "s3.isNewObject";

    static final String PROR_OBJECT_SNAPSHOTS = "isIncludedSnapshots";
    static final String ACL_GROUP_TYPE = "Group";
    static final String ACL_CANONICAL_USER_TYPE = "Canonical User";

    static final String TYPE_DIRECTORY = "application/x-directory";

    // Invalid for metadata names
    private static final char[] HTTP_SEPARATOR_CHARS = new char[]{
            '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', ' ', '\t'};

    abstract void putObject(SyncObject object, String key);

    abstract List<S3ObjectVersion> loadVersions(String key);

    abstract SyncObject loadObject(String key, String versionId);

    SyncObject loadObject(String identifier, boolean includeVersions) throws ObjectNotFoundException {
        if (includeVersions) {
            List<S3ObjectVersion> objectVersions = loadVersions(identifier);
            if (!objectVersions.isEmpty()) {
                // use latest version as object
                S3ObjectVersion object = objectVersions.get(objectVersions.size() - 1);

                object.setProperty(PROP_OBJECT_VERSIONS, objectVersions);

                return object;
            }
            throw new ObjectNotFoundException(identifier);
        } else {
            return loadObject(identifier, null);
        }
    }

    boolean isDirectoryPlaceholder(String contentType, long size) {
        return TYPE_DIRECTORY.equals(contentType) && size == 0;
    }

    void putIntermediateVersions(ListIterator<S3ObjectVersion> versions, final String key) {
        while (versions.hasNext()) {
            S3ObjectVersion version = versions.next();
            try {
                if (!version.isLatest()) {
                    // source has more versions; add any non-current versions that are missing from the target
                    // (current version will be added below)
                    if (version.isDeleteMarker()) {
                        log.debug("[{}#{}]: deleting object in target to replicate delete marker in source.",
                                key, version.getVersionId());
                        delete(key, null);
                    } else {
                        log.debug("[{}#{}]: replicating historical version in target.",
                                key, version.getVersionId());
                        putObject(version, key);
                    }
                }
            } catch (RuntimeException e) {
                throw new RuntimeException(String.format("sync of historical version %s failed", version.getVersionId()), e);
            }
        }
    }

    Map<String, com.emc.ecs.sync.model.ObjectMetadata.UserMetadata> toMetaMap(Map<String, String> sourceMap) {
        Map<String, com.emc.ecs.sync.model.ObjectMetadata.UserMetadata> metaMap = new HashMap<>();
        for (String key : sourceMap.keySet()) {
            metaMap.put(key, new com.emc.ecs.sync.model.ObjectMetadata.UserMetadata(key, sourceMap.get(key)));
        }
        return metaMap;
    }

    Map<String, String> formatUserMetadata(com.emc.ecs.sync.model.ObjectMetadata metadata) {
        Map<String, String> s3meta = new HashMap<>();

        for (String key : metadata.getUserMetadata().keySet()) {
            s3meta.put(filterName(key), filterValue(metadata.getUserMetadataValue(key)));
        }

        return s3meta;
    }

    /**
     * S3 metadata names must be compatible with header naming.  Filter the names so
     * they're acceptable.
     * Per HTTP RFC:<br>
     * <pre>
     * token          = 1*<any CHAR except CTLs or separators>
     * separators     = "(" | ")" | "<" | ">" | "@"
     *                 | "," | ";" | ":" | "\" | <">
     *                 | "/" | "[" | "]" | "?" | "="
     *                 | "{" | "}" | SP | HT
     * <pre>
     *
     * @param name the header name to filter.
     * @return the metadata name filtered to be compatible with HTTP headers.
     */
    private String filterName(String name) {
        try {
            // First, filter out any non-ASCII characters.
            byte[] raw = name.getBytes("US-ASCII");
            String ascii = new String(raw, "US-ASCII");

            // Strip separator chars
            for (char sep : HTTP_SEPARATOR_CHARS) {
                ascii = ascii.replace(sep, '-');
            }

            return ascii;
        } catch (UnsupportedEncodingException e) {
            // should never happen
            throw new RuntimeException("Missing ASCII encoding", e);
        }
    }

    /**
     * S3 sends metadata as HTTP headers, unencoded.  Filter values to be compatible
     * with headers.
     */
    private String filterValue(String value) {
        try {
            // First, filter out any non-ASCII characters.
            byte[] raw = value.getBytes("US-ASCII");
            String ascii = new String(raw, "US-ASCII");

            // Make sure there's no newlines
            ascii = ascii.replace('\n', ' ');

            return ascii;
        } catch (UnsupportedEncodingException e) {
            // should never happen
            throw new RuntimeException("Missing ASCII encoding", e);
        }
    }
}

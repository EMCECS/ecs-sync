/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.storage.s3;

import com.amazonaws.SdkClientException;
import com.emc.ecs.sync.NonRetriableException;
import com.emc.ecs.sync.SkipObjectException;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.AbstractStorage;
import com.emc.ecs.sync.storage.ObjectNotFoundException;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.PerformanceWindow;
import com.emc.ecs.sync.util.SyncUtil;
import com.emc.object.s3.LargeFileUploader;
import com.emc.object.s3.S3Exception;
import com.emc.object.util.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public abstract class AbstractS3Storage<C> extends AbstractStorage<C> {
    private static final Logger log = LoggerFactory.getLogger(AbstractS3Storage.class);

    public static final String PROP_OBJECT_VERSIONS = "s3.objectVersions";
    public static final String PROP_IS_NEW_OBJECT = "s3.isNewObject";
    public static final String PROP_MULTIPART_SOURCE = "s3.multipartSource";
    public static final String PROP_OBJECT_SNAPSHOTS = "s3.isIncludedSnapshots";
    public static final String PROP_SOURCE_ETAG_MATCHES = "s3.sourceEtagMatches";

    public static final String ERROR_CODE_MPU_TERMINATED_EARLY = "S3MpuTerminatedEarly";

    static final String ACL_GROUP_TYPE = "Group";
    static final String ACL_CANONICAL_USER_TYPE = "Canonical User";

    static final String TYPE_DIRECTORY = "application/x-directory";

    public static final String UMD_KEY_SOURCE_MTIME = "x-emc-source-mtime";
    public static final String UMD_KEY_SOURCE_ETAG = "x-emc-source-etag";

    // Invalid for metadata names
    private static final char[] HTTP_SEPARATOR_CHARS = new char[]{
            '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', ' ', '\t'};

    protected PerformanceWindow sourceReadWindow;

    abstract void putObject(SyncObject object, String key);

    abstract List<S3ObjectVersion> loadVersions(String key);

    abstract SyncObject loadObject(String key, String versionId);

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);
        if (target == this && source != null) sourceReadWindow = source.getReadWindow();
    }

    /**
     * Checks for <code>x-emc-source-mtime</code> and <code>x-emc-source-etag</code> in target user metadata,
     * to determine more accurately if target object is up-to-date.
     * If x-emc-source-mtime is not present, defers to {@link super#skipIfExists(ObjectContext, SyncObject)}
     */
    @Override
    protected void skipIfExists(ObjectContext objectContext, SyncObject targetObject) {
        String tSourceMtime = targetObject.getMetadata().getUserMetadataValue(UMD_KEY_SOURCE_MTIME);
        if (tSourceMtime == null) {
            // if not present, defer to default (super) skipIfExists()
            log.debug("target missing x-emc-source-mtime; deferring to default skipIfExists");
            super.skipIfExists(objectContext, targetObject);
        } else {
            // if target has x-emc-source-mtime, and source mtime is not newer, we can skip it
            long sourceMtime = objectContext.getObject().getMetadata().getModificationTime().getTime();
            if (Long.parseLong(tSourceMtime) >= sourceMtime) {
                // target is same or newer than source
                log.debug("target x-emc-source-mtime ({}}) >= source mtime ({}}); skipping object", tSourceMtime, sourceMtime);
                throw new SkipObjectException(String.format("x-emc-source-mtime (%s) >= source mtime (%d)", tSourceMtime, sourceMtime));
            }

            // otherwise, if target has x-emc-source-etag that matches, we need to record that so updateObject knows to
            // only do a metadata update (since it does not have access to the loaded target metadata)
            String tSourceEtag = targetObject.getMetadata().getUserMetadataValue(UMD_KEY_SOURCE_ETAG);
            String sourceEtag = objectContext.getObject().getMetadata().getHttpEtag();

            if (tSourceEtag != null && tSourceEtag.equals(sourceEtag)) {
                // target data matches the source
                objectContext.getObject().setProperty(PROP_SOURCE_ETAG_MATCHES, Boolean.TRUE);
            }
        }
    }

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
        // First, filter out any non-ASCII characters.
        byte[] raw = name.getBytes(StandardCharsets.US_ASCII);
        String ascii = new String(raw, StandardCharsets.US_ASCII);

        // Strip separator chars
        for (char sep : HTTP_SEPARATOR_CHARS) {
            ascii = ascii.replace(sep, '-');
        }

        return ascii;
    }

    /**
     * S3 sends metadata as HTTP headers, unencoded.  Filter values to be compatible
     * with headers.
     */
    private String filterValue(String value) {
        // First, filter out any non-ASCII characters.
        byte[] raw = value.getBytes(StandardCharsets.US_ASCII);
        String ascii = new String(raw, StandardCharsets.US_ASCII);

        // Make sure there's no newlines
        ascii = ascii.replace('\n', ' ');

        return ascii;
    }

    protected boolean shouldAbortMpu(LargeFileUploader uploader, RuntimeException uploadException) {
        Throwable rootCause = SyncUtil.getCause(uploadException);
        boolean isMpuPresent = uploader.getResumeContext() != null && uploader.getResumeContext().getUploadId() != null;
        boolean isRetriable = rootCause instanceof IOException
                || (rootCause instanceof S3Exception && ((S3Exception) rootCause).getHttpCode() >= 500 && ((S3Exception) rootCause).getHttpCode() != 501)
                || (rootCause instanceof SdkClientException && ((SdkClientException) rootCause).isRetryable())
                || (rootCause instanceof NonRetriableException && Objects.equals(((NonRetriableException) rootCause).getErrorCode(), ERROR_CODE_MPU_TERMINATED_EARLY));

        return isMpuPresent && !isRetriable;
    }

    protected class ByteTransferListener implements ProgressListener {
        private final SyncObject object;

        ByteTransferListener(SyncObject object) {
            this.object = object;
        }

        @Override
        public void progress(long completed, long total) {
        }

        @Override
        public void transferred(long size) {
            if (sourceReadWindow != null) sourceReadWindow.increment(size);
            if (options.isMonitorPerformance()) getWriteWindow().increment(size);
            synchronized (object) {
                // these events will include XML payload for MPU (no way to differentiate)
                // do not set bytesRead to more then the object size
                object.setBytesRead(object.getBytesRead() + size);
                if (object.getBytesRead() > object.getMetadata().getContentLength())
                    object.setBytesRead(object.getMetadata().getContentLength());
            }
        }
    }
}

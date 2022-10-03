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

import com.emc.ecs.sync.NonRetriableException;
import com.emc.ecs.sync.SkipObjectException;
import com.emc.ecs.sync.SyncTask;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.AwsS3Config;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import com.emc.ecs.sync.config.storage.S3ConfigurationException;
import com.emc.ecs.sync.config.storage.S3ConfigurationException.Error;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.storage.ObjectNotFoundException;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.azure.AzureBlobStorage;
import com.emc.ecs.sync.storage.azure.BlobSyncObject;
import com.emc.ecs.sync.util.*;
import com.emc.object.Protocol;
import com.emc.object.Range;
import com.emc.object.s3.*;
import com.emc.object.s3.bean.*;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.lfu.LargeFileMultipartSource;
import com.emc.object.s3.lfu.LargeFileUpload;
import com.emc.object.s3.lfu.LargeFileUploaderResumeContext;
import com.emc.object.s3.request.*;
import com.emc.object.util.ProgressInputStream;
import com.emc.rest.smart.ecs.Vdc;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;

import static com.emc.ecs.sync.config.storage.EcsS3Config.MIN_PART_SIZE_MB;

public class EcsS3Storage extends AbstractS3Storage<EcsS3Config> implements OptionChangeListener {
    private static final Logger log = LoggerFactory.getLogger(EcsS3Storage.class);

    // timed operations
    public static final String OPERATION_LIST_OBJECTS = "EcsS3ListObjects";
    public static final String OPERATION_LIST_VERSIONS = "EcsS3ListVersions";
    public static final String OPERATION_HEAD_OBJECT = "EcsS3HeadObject";
    public static final String OPERATION_GET_ACL = "EcsS3GetAcl";
    public static final String OPERATION_OPEN_DATA_STREAM = "EcsS3OpenDataStream";
    public static final String OPERATION_PUT_OBJECT = "EcsS3PutObject";
    public static final String OPERATION_MPU = "EcsS3MultipartUpload";
    public static final String OPERATION_DELETE_VERSIONS = "EcsS3DeleteVersions";
    public static final String OPERATION_DELETE_OBJECT = "EcsS3DeleteObject";
    public static final String OPERATION_UPDATE_METADATA = "EcsS3UpdateMetadata";
    public static final String OPERATION_REMOTE_COPY = "EcsS3RemoteCopy";

    private S3Client s3;
    private EcsS3Storage source;
    private EnhancedThreadPoolExecutor mpuThreadPool;

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        if (config.getProtocol() == null) throw new ConfigurationException("protocol is required");
        if (config.getHost() == null && (config.getVdcs() == null || config.getVdcs().length == 0))
            throw new ConfigurationException("at least one host is required");
        if (config.getAccessKey() == null) throw new ConfigurationException("access-key is required");
        if (config.getSecretKey() == null) throw new ConfigurationException("secret-key is required");
        if (config.getBucketName() == null) throw new ConfigurationException("bucket is required");
        if (!config.getBucketName().matches("[A-Za-z0-9._-]+"))
            throw new ConfigurationException(config.getBucketName() + " is not a valid bucket name");

        S3Config s3Config;
        if (config.isEnableVHosts()) {
            if (config.getHost() == null)
                throw new ConfigurationException("you must provide a single host to enable v-host buckets");
            try {
                String portStr = config.getPort() > 0 ? ":" + config.getPort() : "";
                URI endpoint = new URI(String.format("%s://%s%s", config.getProtocol().toString(), config.getHost(), portStr));
                s3Config = new S3Config(endpoint);
            } catch (URISyntaxException e) {
                throw new ConfigurationException("invalid endpoint", e);
            }
        } else {
            List<Vdc> vdcs = new ArrayList<>();
            if (config.getVdcs() != null && getConfig().getVdcs().length > 0) {
                for (String vdcString : config.getVdcs()) {
                    Matcher matcher = EcsS3Config.VDC_PATTERN.matcher(vdcString);
                    if (matcher.matches()) {
                        Vdc vdc = new Vdc(matcher.group(2).split(","));
                        if (matcher.group(1) != null) vdc.setName(matcher.group(1));
                        vdcs.add(vdc);
                    } else {
                        throw new ConfigurationException("invalid VDC format: " + vdcString);
                    }
                }
            } else {
                vdcs.add(new Vdc(config.getHost()));
            }
            s3Config = new S3Config(Protocol.valueOf(config.getProtocol().toString().toUpperCase()), vdcs.toArray(new Vdc[0]));
            if (config.getPort() > 0) s3Config.setPort(config.getPort());
            s3Config.setSmartClient(config.isSmartClientEnabled());
        }
        s3Config.withIdentity(config.getAccessKey()).withSecretKey(config.getSecretKey()).withSessionToken(config.getSessionToken());
        s3Config.setProperty(ClientConfig.PROPERTY_CONNECT_TIMEOUT, config.getSocketConnectTimeoutMs());
        s3Config.setProperty(ClientConfig.PROPERTY_READ_TIMEOUT, config.getSocketReadTimeoutMs());

        if (config.isGeoPinningEnabled()) {
            if (s3Config.getVdcs() == null || s3Config.getVdcs().size() < 3)
                throw new ConfigurationException("geo-pinning should only be enabled for 3+ VDCs!");
            s3Config.setGeoPinningEnabled(true);
        }

        if (config.isApacheClientEnabled()) {
            s3 = new S3JerseyClient(s3Config);
        } else {
            System.setProperty("http.maxConnections", "1000");
            s3 = new S3JerseyClient(s3Config, new URLConnectionClientHandler());
        }

        boolean bucketExists;
        try {
            bucketExists = s3.bucketExists(config.getBucketName());
        } catch (S3Exception e) {
            // Note: the above call should *not* throw an exception unless something is wrong in the environment
            // (network/service issue)
            throw new ConfigurationException("cannot determine if " + getRole() + " bucket exists", e);
        }

        boolean bucketHasVersions = false;
        if (bucketExists && config.isIncludeVersions()) {
            // check if versioning has ever been enabled on the bucket (versions will not be collected unless required)
            VersioningConfiguration versioningConfig = s3.getBucketVersioning(config.getBucketName());
            List<VersioningConfiguration.Status> versionedStates = Arrays.asList(VersioningConfiguration.Status.Enabled, VersioningConfiguration.Status.Suspended);
            bucketHasVersions = versionedStates.contains(versioningConfig.getStatus());
        }

        if (config.getKeyPrefix() == null) config.setKeyPrefix(""); // make sure keyPrefix isn't null

        if (target == this) {
            // create bucket if it doesn't exist
            if (!bucketExists && config.isCreateBucket()) {
                s3.createBucket(config.getBucketName());
                bucketExists = true;
                if (config.isIncludeVersions()) {
                    s3.setBucketVersioning(config.getBucketName(), new VersioningConfiguration().withStatus(VersioningConfiguration.Status.Enabled));
                    bucketHasVersions = true;
                }
            }

            // make sure MPU settings are valid
            if (config.getMpuPartSizeMb() < MIN_PART_SIZE_MB) {
                log.warn("{}MB is below the minimum MPU part size of {}MB. the minimum will be used instead",
                        config.getMpuPartSizeMb(), MIN_PART_SIZE_MB);
                config.setMpuPartSizeMb(MIN_PART_SIZE_MB);
            }
        }

        // make sure bucket exists
        if (!bucketExists)
            throw new ConfigurationException("The bucket " + config.getBucketName() + " does not exist.");

        //Bucket Access Validation
        if (target == this)
            validateBucketWriteAccess(config.getBucketName());

        // if syncing versions, make sure plugins support it and bucket has versioning enabled
        if (config.isIncludeVersions()) {
//            if (!(source instanceof AbstractS3Storage && target instanceof AbstractS3Storage))
//                throw new ConfigurationException("Version migration is only supported between two S3 plugins");
            if (!(target instanceof AbstractS3Storage)) {
                throw new ConfigurationException("Version migration is only supported when target is S3 plugins");
            }

            if (!bucketHasVersions)
                throw new ConfigurationException("The specified bucket does not have versioning enabled.");
        }

        // if remote copy, make sure source is also S3
        if (config.isRemoteCopy()) {
            if (source instanceof EcsS3Storage) this.source = (EcsS3Storage) source;
            else throw new ConfigurationException("Remote copy is only supported between two ECS-S3 plugins");
        }

        mpuThreadPool = new EnhancedThreadPoolExecutor(options.getThreadCount(), new LinkedBlockingDeque<>(100), "mpu-pool");
    }

    @Override
    public void optionsChanged(SyncOptions options) {
        // if the thread-count changes, modify the MPU thread pool accordingly
        mpuThreadPool.resizeThreadPool(options.getThreadCount());
    }

    @Override
    public void close() {
        try {
            if (mpuThreadPool != null) mpuThreadPool.shutdown();
        } catch (Exception e) {
            log.warn("could not shutdown MPU thread pool", e);
        }
        try {
            if (s3 != null) s3.destroy();
        } catch (Exception e) {
            log.warn("could not destroy S3 client", e);
        }
        super.close();
    }

    @Override
    public String getRelativePath(String identifier, boolean directory) {
        String relativePath = identifier;
        if (relativePath.startsWith(config.getKeyPrefix()))
            relativePath = relativePath.substring(config.getKeyPrefix().length());
        // remove trailing slash from directories
        if (directory && relativePath.endsWith("/"))
            relativePath = relativePath.substring(0, relativePath.length() - 1);
        return relativePath;
    }

    @Override
    public String getIdentifier(String relativePath, boolean directory) {
        if (relativePath == null || relativePath.length() == 0) return config.getKeyPrefix();
        String identifier = config.getKeyPrefix() + relativePath;
        // append trailing slash for directories
        if (directory) identifier += "/";
        return identifier;
    }

    @Override
    protected ObjectSummary createSummary(String identifier) {
        long size = 0;
        if (!config.isRemoteCopy() || options.isSyncMetadata()) { // for pure remote-copy; avoid HEAD requests
            S3ObjectMetadata s3Metadata = getS3Metadata(identifier, null);
            size = s3Metadata.getContentLength();
        }
        return new ObjectSummary(identifier, false, size);
    }

    @Override
    public Iterable<ObjectSummary> allObjects() {
        if (config.isIncludeVersions()) {
            return () -> new CombinedIterator<>(Arrays.asList(new PrefixIterator(config.getKeyPrefix()), new DeletedObjectIterator(config.getKeyPrefix())));
        } else {
            return () -> new PrefixIterator(config.getKeyPrefix());
        }
    }

    // TODO: implement directoryMode, using prefix+delimiter
    @Override
    public Iterable<ObjectSummary> children(ObjectSummary parent) {
        return Collections.emptyList();
    }

    @Override
    public SyncObject loadObject(String identifier) throws ObjectNotFoundException {
        return loadObject(identifier, config.isIncludeVersions());
    }

    @Override
    SyncObject loadObject(final String key, final String versionId) {
        // load metadata
        com.emc.ecs.sync.model.ObjectMetadata metadata;
        try {
            if (!config.isRemoteCopy() || options.isSyncMetadata()) {
                metadata = syncMetaFromS3Meta(getS3Metadata(key, versionId));
            } else {
                metadata = new ObjectMetadata(); // for pure remote-copy; avoid HEAD requests
            }
        } catch (S3Exception e) {
            if (e.getHttpCode() == 404) {
                throw new ObjectNotFoundException(key + (versionId == null ? "" : " (versionId=" + versionId + ")"));
            } else {
                throw e;
            }
        }

        SyncObject object;
        if (versionId == null) {
            object = new SyncObject(this, getRelativePath(key, metadata.isDirectory()), metadata);
        } else {
            object = new S3ObjectVersion(this, getRelativePath(key, metadata.isDirectory()), metadata);
        }

        object.setLazyAcl(() -> syncAclFromS3Acl(getS3Acl(key, versionId)));

        object.setLazyStream(() -> getS3DataStream(key, versionId));

        object.setProperty(AbstractS3Storage.PROP_MULTIPART_SOURCE, getMultipartSource(key, versionId, metadata));

        return object;
    }

    @Override
    List<S3ObjectVersion> loadVersions(final String key) {
        List<S3ObjectVersion> versions = new ArrayList<>();

        boolean directory = false; // delete markers won't have any metadata, so keep track of directory status
        for (AbstractVersion aVersion : getS3Versions(key)) {
            S3ObjectVersion version;
            if (aVersion instanceof DeleteMarker) {
                version = new S3ObjectVersion(this, getRelativePath(key, directory),
                        new com.emc.ecs.sync.model.ObjectMetadata().withModificationTime(aVersion.getLastModified())
                                .withContentLength(0).withDirectory(directory));
                version.setDeleteMarker(true);
            } else {
                version = (S3ObjectVersion) loadObject(key, aVersion.getVersionId());
                directory = version.getMetadata().isDirectory();
                version.setETag(((Version) aVersion).getETag());

            }
            version.setVersionId(aVersion.getVersionId());
            version.setLatest(aVersion.isLatest());
            versions.add(version);
        }

        versions.sort(new S3VersionComparator());

        return versions;
    }

    public LargeFileMultipartSource getMultipartSource(String key, String versionId, ObjectMetadata metadata) {
        return new LargeFileMultipartSource() {
            @Override
            public long getTotalSize() {
                return metadata.getContentLength();
            }

            @Override
            public InputStream getCompleteDataStream() {
                InputStream dataStream = s3.getObject(
                        new GetObjectRequest<>(config.getBucketName(), key)
                                .withVersionId(versionId)
                                .withIfMatch(metadata.getHttpEtag()),
                        InputStream.class
                ).getObject();
                // apply throttles
                dataStream = SyncUtil.throttleStream(dataStream, getSyncJob());
                return dataStream;
            }

            @Override
            public InputStream getPartDataStream(long offset, long length) {
                InputStream dataStream = s3.getObject(
                        new GetObjectRequest<>(config.getBucketName(), key)
                                .withVersionId(versionId)
                                .withRange(Range.fromOffsetLength(offset, length))
                                .withIfMatch(metadata.getHttpEtag()),
                        InputStream.class
                ).getObject();
                // apply throttles
                dataStream = SyncUtil.throttleStream(dataStream, getSyncJob());
                return dataStream;
            }
        };
    }

    @Override
    public String createObject(SyncObject object) {
        object.setProperty(PROP_IS_NEW_OBJECT, Boolean.TRUE);
        return super.createObject(object);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateObject(final String identifier, SyncObject object) {
        try {
            // skip the root of the bucket since it obviously exists
            if ("".equals(identifier)) {
                log.debug("Target is bucket root; skipping");
                throw new SkipObjectException("Target is bucket root");
            }

            // check early on to see if we should ignore directories
            if (!config.isPreserveDirectories() && object.getMetadata().isDirectory()) {
                log.debug("{} is a directory and preserveDirectories is false; skipping", object.getRelativePath());
                throw new SkipObjectException("source object is a directory and preserveDirectories is false");
            }

            if (config.isIncludeVersions() && object instanceof BlobSyncObject) {
                List<BlobSyncObject> sourceBlobSnapshots = (List<BlobSyncObject>) object.getProperty(AzureBlobStorage.PROP_BLOB_SNAPSHOTS);
                ListIterator<S3ObjectVersion> targetVersionItor = loadVersions(identifier).listIterator();

                if (sourceBlobSnapshots.size() == 0) {
                    throw new RuntimeException("Failed to get blob snapshots: " + identifier);
                }

                boolean isDifferent = false;
                List<S3ObjectVersion> targetVersions = new ArrayList<>();
                if (targetVersionItor.hasNext()) {
                    while (targetVersionItor.hasNext()) {
                        S3ObjectVersion targetVersion = targetVersionItor.next();

                        if (targetVersion.isDeleteMarker()) {
                            continue;
                        }
                        targetVersions.add(targetVersion);
                    }
                }

                if (targetVersions.size() != 0) {
                    log.debug("sourceBlobSnapshots {} targetVersions {}", sourceBlobSnapshots.size(), targetVersions.size());
                    if (sourceBlobSnapshots.size() == targetVersions.size()) {
                        for (int i = 0; i < sourceBlobSnapshots.size(); i++) {
                            //TODO: md5sum need to be compared here
                            if (sourceBlobSnapshots.get(i).getMetadata().getContentLength()
                                    != targetVersions.get(i).getMetadata().getContentLength()) {
                                isDifferent = true;
                                break;
                            }
                        }
                    } else {
                        isDifferent = true;
                    }
                }

                // if there are some version in target and is different with source, need to delete it before putObject
                if (isDifferent) {
                    log.debug("source is different with target, need to remove target first");

                    final List<ObjectKey> deleteVersions = new ArrayList<>();
                    while (targetVersionItor.hasNext()) {
                        targetVersionItor.next();
                    }
                    // move cursor to end
                    while (targetVersionItor.hasPrevious()) {
                        // go in reverse order
                        S3ObjectVersion version = targetVersionItor.previous();
                        deleteVersions.add(new ObjectKey(identifier, version.getVersionId()));
                    }
                    operationWrapper((Function<Void>) () -> {
                        s3.deleteObjects(new DeleteObjectsRequest(config.getBucketName()).withKeys(deleteVersions));
                        return null;
                    }, OPERATION_DELETE_VERSIONS, object, identifier);

                    operationWrapper((Function<Void>) () -> {
                        s3.deleteObject(config.getBucketName(), identifier);
                        return null;
                    }, OPERATION_DELETE_OBJECT, object, identifier);
                } else if (targetVersions.size() != 0) {
                    log.debug("Source and target versions are the same.  Skipping {}", object.getRelativePath());
                    return;
                }

                for (BlobSyncObject blobSyncObject : sourceBlobSnapshots) {
                    log.debug("[{}#{}]: replicating historical version in target.", identifier, blobSyncObject.getSnapshotId());
                    putObject(blobSyncObject, identifier);
                }
                return;
            }

            List<S3ObjectVersion> sourceVersionList = (List<S3ObjectVersion>) object.getProperty(PROP_OBJECT_VERSIONS);
            if (config.isIncludeVersions() && sourceVersionList != null) {
                ListIterator<S3ObjectVersion> sourceVersions = sourceVersionList.listIterator();
                ListIterator<S3ObjectVersion> targetVersions = loadVersions(identifier).listIterator();

                boolean newVersions = false, replaceVersions = false;
                if (options.isForceSync()) {
                    replaceVersions = true;
                } else {

                    // special workaround for bug where objects are listed, but they have no versions
                    if (sourceVersions.hasNext()) {

                        // check count and etag/delete-marker to compare version chain
                        while (sourceVersions.hasNext()) {
                            S3ObjectVersion sourceVersion = sourceVersions.next();

                            if (targetVersions.hasNext()) {
                                S3ObjectVersion targetVersion = targetVersions.next();

                                if (sourceVersion.isDeleteMarker()) {

                                    if (!targetVersion.isDeleteMarker()) replaceVersions = true;
                                } else {

                                    if (targetVersion.isDeleteMarker()) replaceVersions = true;

                                    else if (!sourceVersion.getETag().equals(targetVersion.getETag()))
                                        replaceVersions = true; // different checksum
                                }

                            } else if (!replaceVersions) { // source has new versions, but existing target versions are ok
                                newVersions = true;
                                sourceVersions.previous(); // back up one
                                putIntermediateVersions(sourceVersions, identifier); // add any new intermediary versions (current is added below)
                            }
                        }

                        if (targetVersions.hasNext()) replaceVersions = true; // target has more versions

                        if (!newVersions && !replaceVersions) {
                            log.info("Source and target versions are the same.  Skipping {}", object.getRelativePath());
                            return;
                        }
                    }
                }

                // something's off; must delete all versions of the object
                if (replaceVersions) {
                    log.info("[{}]: version history differs between source and target; re-placing target version history with that from source.",
                            object.getRelativePath());

                    // collect versions in target
                    final List<ObjectKey> deleteVersions = new ArrayList<>();
                    while (targetVersions.hasNext()) targetVersions.next(); // move cursor to end
                    while (targetVersions.hasPrevious()) { // go in reverse order
                        S3ObjectVersion version = targetVersions.previous();
                        deleteVersions.add(new ObjectKey(identifier, version.getVersionId()));
                    }

                    // batch delete all versions in target
                    log.debug("[{}]: deleting all versions in target", object.getRelativePath());
                    if (!deleteVersions.isEmpty()) {
                        operationWrapper((Function<Void>) () -> {
                            s3.deleteObjects(new DeleteObjectsRequest(config.getBucketName()).withKeys(deleteVersions));
                            return null;
                        }, OPERATION_DELETE_VERSIONS, object, identifier);
                    }

                    // replay version history in target
                    while (sourceVersions.hasPrevious()) sourceVersions.previous(); // move cursor to beginning
                    putIntermediateVersions(sourceVersions, identifier);
                }
            }

            // at this point we know we are going to write the object
            // Put [current object version]
            if (object instanceof S3ObjectVersion && ((S3ObjectVersion) object).isDeleteMarker()) {

                // object has version history, but is currently deleted
                log.debug("[{}]: deleting object in target to replicate delete marker in source.", object.getRelativePath());
                operationWrapper((Function<Void>) () -> {
                    s3.deleteObject(config.getBucketName(), identifier);
                    return null;
                }, OPERATION_DELETE_OBJECT, object, identifier);
            } else {
                putObject(object, identifier);

                // if object has new metadata after the stream (i.e. encryption checksum), we must update S3 again
                if (object.isPostStreamUpdateRequired()) {
                    // can't modify objects during a remote copy
                    if (config.isRemoteCopy())
                        throw new RuntimeException("You cannot apply a transforming filter on a remote-copy");

                    log.debug("[{}]: updating metadata after sync as required", object.getRelativePath());
                    final CopyObjectRequest cReq = new CopyObjectRequest(config.getBucketName(), identifier, config.getBucketName(), identifier);
                    cReq.setObjectMetadata(s3MetaFromSyncMeta(object.getMetadata()));
                    operationWrapper((Function<Void>) () -> {
                        s3.copyObject(cReq);
                        return null;
                    }, OPERATION_UPDATE_METADATA, object, identifier);
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to store object: " + e, e);
        }
    }

    @Override
    void putObject(SyncObject obj, final String targetKey) {
        boolean isNew = obj.getProperty(PROP_IS_NEW_OBJECT) != null && (Boolean) obj.getProperty(PROP_IS_NEW_OBJECT);
        S3ObjectMetadata om;
        if (options.isSyncMetadata()) {
            om = s3MetaFromSyncMeta(obj.getMetadata());
        } else {
            om = new S3ObjectMetadata();
            om.setContentLength(obj.getMetadata().getContentLength());
        }

        if (obj.getMetadata().isDirectory()) om.setContentType(TYPE_DIRECTORY);
        AccessControlList acl = options.isSyncAcl() ? s3AclFromSyncAcl(obj.getAcl(), options.isIgnoreInvalidAcls()) : null;

        // differentiate single PUT or multipart upload
        long thresholdSize = (long) config.getMpuThresholdMb() * 1024 * 1024; // convert from MB
        if (config.isRemoteCopy()) {
            String sourceKey = source.getIdentifier(obj.getRelativePath(), obj.getMetadata().isDirectory());
            final CopyObjectRequest copyRequest = new CopyObjectRequest(source.getConfig().getBucketName(), sourceKey, config.getBucketName(), targetKey);
            if (obj instanceof S3ObjectVersion) copyRequest.setSourceVersionId(((S3ObjectVersion) obj).getVersionId());
            if (options.isSyncMetadata()) copyRequest.setObjectMetadata(om);
            else if (Integer.valueOf(0).equals(obj.getProperty(SyncTask.PROP_FAILURE_COUNT)))
                copyRequest.setIfTargetNoneMatch("*"); // special case for pure remote-copy (except on retries)
            if (options.isSyncAcl()) copyRequest.setAcl(acl);

            try {
                operationWrapper((Function<Void>) () -> {
                    s3.copyObject(copyRequest);
                    return null;
                }, OPERATION_REMOTE_COPY, obj, targetKey);
            } catch (S3Exception e) {
                // special case for pure remote-copy; on 412, object already exists in target
                if (e.getHttpCode() != 412) throw e;
            }

        } else if ((!options.isSyncData() && !isNew)
                || (obj.getProperty(PROP_SOURCE_ETAG_MATCHES) != null && (Boolean) obj.getProperty(PROP_SOURCE_ETAG_MATCHES))) {
            // the object exists in target and we are not syncing data, so do a metadata-update
            CopyObjectRequest request = new CopyObjectRequest(config.getBucketName(), targetKey, config.getBucketName(), targetKey);
            request.withObjectMetadata(om);

            if (options.isSyncAcl()) request.setAcl(acl);

            CopyObjectResult result = operationWrapper(() -> s3.copyObject(request), OPERATION_UPDATE_METADATA, obj, targetKey);

            log.debug("Updated metadata for {}, etag: {}", targetKey, result.getETag());
        } else if (!config.isMpuEnabled() || obj.getMetadata().getContentLength() < thresholdSize) {
            Object data;
            if (obj.getMetadata().isDirectory() || !options.isSyncData()) {
                // this is either a directory (has no data), or it's a new object and we have been configured to *not* copy the data
                data = new byte[0];
            } else {
                if (options.isMonitorPerformance()) data = new ProgressInputStream(obj.getDataStream(),
                        new PerformanceListener(getWriteWindow()));
                else data = obj.getDataStream();
            }

            // work around Jersey's insistence on using chunked transfer with "identity" content-encoding
            if (om.getContentLength() == 0 && "identity".equals(om.getContentEncoding()))
                om.setContentEncoding(null);

            final PutObjectRequest req = new PutObjectRequest(config.getBucketName(), targetKey, data).withObjectMetadata(om);

            if (options.isSyncAcl()) req.setAcl(acl);

            try {
                // only use If-None-Match if it is the first try and not forceSync
                if (!options.isForceSync() && Integer.valueOf(0).equals(obj.getProperty(SyncTask.PROP_FAILURE_COUNT))) {
                    if (obj.getMetadata().getHttpEtag() != null) {
                        req.withIfNoneMatch(obj.getMetadata().getHttpEtag());
                    } else {
                        log.debug("No ETag available to use If-None-Match on PutObject for object {}.", targetKey);
                    }
                }
                PutObjectResult result = operationWrapper(() -> s3.putObject(req), OPERATION_PUT_OBJECT, obj, targetKey);
                log.debug("Wrote {} etag: {}", targetKey, result.getETag());
            } catch (S3Exception e) {
                if (e.getHttpCode() == 412) {
                    log.debug("Skip writing target object {} as ETag is unchanged.", targetKey);
                    if (options.isSyncMetadata() || options.isSyncAcl()) {
                        CopyObjectRequest copyRequest = new CopyObjectRequest(config.getBucketName(), targetKey, config.getBucketName(), targetKey);
                        if (options.isSyncMetadata()) {
                            copyRequest.setObjectMetadata(om);
                        }
                        if (options.isSyncAcl()) {
                            copyRequest.setAcl(acl);
                        }
                        CopyObjectResult result = operationWrapper(() -> s3.copyObject(copyRequest), OPERATION_UPDATE_METADATA, obj, targetKey);
                        log.debug("Updated metadata for {}, etag: {}", targetKey, result.getETag());
                    }
                } else throw e;
            }
        } else {
            // MPU is enabled and content-length is above threshold
            LargeFileUploader uploader;

            // TODO: we should not reference another plugin from this one - where should this constant live??
            File file = (File) obj.getProperty("filesystem.file");
            if (file != null) {
                // we can read file parts in parallel
                uploader = new LargeFileUploader(s3, config.getBucketName(), targetKey, file);
                // because we are bypassing the source data stream, we need to make
                // sure to update the source-read and target-write windows, as well as the object's bytes-read
                // TODO: apply bandwidth throttle here
                uploader.setProgressListener(new ByteTransferListener(obj));
            } else if (obj.getProperty(AbstractS3Storage.PROP_MULTIPART_SOURCE) != null) {
                // our source object supports parallel streams
                // TODO: need to generalize a parallel stream source as a 1st-class concept in ecs-sync
                //       i.e. maybe embed logic in SyncObject so that streams are wrapped properly and checksum
                //       calculation is automatic
                uploader = new LargeFileUploader(s3, config.getBucketName(), targetKey,
                        (LargeFileMultipartSource) obj.getProperty(AbstractS3Storage.PROP_MULTIPART_SOURCE));
                // because we are bypassing the source data stream, we need to make
                // sure to update the source-read and target-write windows, as well as the object's bytes-read
                uploader.setProgressListener(new ByteTransferListener(obj));
            } else {
                InputStream dataStream = obj.getDataStream();
                if (options.isMonitorPerformance())
                    dataStream = new ProgressInputStream(dataStream, new PerformanceListener(getWriteWindow()));
                uploader = new LargeFileUploader(s3, config.getBucketName(), targetKey, dataStream, obj.getMetadata().getContentLength());
                uploader.setCloseStream(true);
            }
            uploader.withPartSize((long) config.getMpuPartSizeMb() * 1024 * 1024).withMpuThreshold((long) config.getMpuThresholdMb() * 1024 * 1024);
            uploader.setObjectMetadata(om);

            if (options.isSyncAcl()) uploader.setAcl(acl);

            // in this else block, we already know MPU is enabled and content-length is above threshold
            // if resume-mpu is enabled, try to find an existing uploadId to resume
            if (config.isMpuResumeEnabled()) {
                uploader.setAbortMpuOnFailure(false); // see additional MPU abort logic in the catch block below
                String uploadId = getLatestMultipartUploadId(targetKey, obj.getMetadata().getModificationTime());
                if (uploadId != null) {
                    uploader.setResumeContext(new LargeFileUploaderResumeContext().withUploadId(uploadId));
                }
                // TODO: list all possible states and expected behavior
                //       i.e. when should we: overwrite?  abort?  retry?
            }

            // have the uploader use the shared MPU thread pool for part uploads
            uploader.setExecutorService(new SharedThreadPoolBackedExecutor(mpuThreadPool));

            final LargeFileUpload upload = uploader.uploadAsync();
            try {
                operationWrapper((Function<Void>) () -> {

                    if (config.isMpuResumeEnabled()) {
                        // resume is enabled, so we must detect if the sync job was stopped, and pause the upload
                        while (true) {
                            try {
                                if (syncJob == null || syncJob.isRunning()) {
                                    upload.waitForCompletion(5, TimeUnit.SECONDS);
                                    break; // upload is done
                                } else {
                                    // sync job was stopped early. pause the upload, so it can be resumed on a subsequent run
                                    // TODO: how can we communicate MPU state to calling code?
                                    upload.pause();
                                    throw new NonRetriableException(ERROR_CODE_MPU_TERMINATED_EARLY, "MPU was paused due to sync job early termination");
                                }
                            } catch (TimeoutException ignored) {
                                // upload is still in progress
                            }
                        }
                    } else {
                        // resume is not enabled, so wait for the entire upload to finish
                        upload.waitForCompletion();
                    }

                    return null;
                }, OPERATION_MPU, obj, targetKey);
                log.debug("Wrote {} as MPU; etag: {}", targetKey, uploader.getETag());
            } catch (RuntimeException uploadException) {
                // additional MPU abort logic when resume is enabled
                // (if the error here is not retriable, we should manually abort the MPU)
                if (shouldAbortMpu(uploader, uploadException)) {
                    log.info("Aborting MPU for {} due to non-resumable exception: {}", uploader.getKey(), uploadException);
                    s3.abortMultipartUpload(new AbortMultipartUploadRequest(uploader.getBucket(), uploader.getKey(), uploader.getResumeContext().getUploadId()));
                } else {
                    log.debug("Not aborting MPU for {} due to resumable exception: {}", uploader.getKey(), uploadException);
                }
                throw uploadException;
            }
        }
    }

    /*
     * returns the latest (most recently initiated) MPU for the configured bucket/key that was initiated after
     * initiatedAfter, or null if none is found.
     */
    private String getLatestMultipartUploadId(String objectKey, Date initiatedAfter) {
        Upload latestUpload = null;
        try {
            ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(config.getBucketName()).withPrefix(objectKey);
            ListMultipartUploadsResult result = null;
            do {
                if (result == null) {
                    result = s3.listMultipartUploads(request);
                } else {
                    result = s3.listMultipartUploads(request.withKeyMarker(result.getNextKeyMarker()).withUploadIdMarker(result.getNextUploadIdMarker()));
                }
                for (Upload upload : result.getUploads()) {
                    // filter out non-matching keys
                    if (!upload.getKey().equals(objectKey)) continue;
                    // filter out stale uploads
                    if (initiatedAfter != null && upload.getInitiated().before(initiatedAfter)) {
                        log.debug("Stale Upload detected ({}): Initiated time {} shouldn't be earlier than {}.",
                                upload.getUploadId(), upload.getInitiated(), initiatedAfter);
                        continue;
                    }
                    if (latestUpload != null) {
                        if (upload.getInitiated().after(latestUpload.getInitiated())) {
                            log.debug("found newer matching upload ({} : {})", upload.getUploadId(), upload.getInitiated());
                            latestUpload = upload;
                        } else {
                            log.debug("Skipping upload ({} : {}) because a newer one was found", upload.getUploadId(), upload.getInitiated());
                        }
                    } else {
                        log.debug("found matching upload ({} : {})", upload.getUploadId(), upload.getInitiated());
                        latestUpload = upload;
                    }
                }
            } while (result.isTruncated());
        } catch (Exception e) {
            log.warn("Error retrieving MPU uploads in target", e);
            latestUpload = null;
        }
        if (latestUpload == null) return null;
        return latestUpload.getUploadId();
    }

    @Override
    public void delete(final String identifier, SyncObject object) {
        operationWrapper((Function<Void>) () -> {
            DeleteObjectRequest request = new DeleteObjectRequest(config.getBucketName(), identifier);
            if (object != null) {
                if (object.getMetadata().getHttpEtag() != null)
                    request.withIfMatch(object.getMetadata().getHttpEtag());
                else
                    log.info("No ETag available to use If-Match on delete for object {}.", identifier);
                request.withIfUnmodifiedSince(object.getMetadata().getModificationTime());
            }
            try {
                s3.deleteObject(request);
            } catch (S3Exception e) {
                if (e.getHttpCode() == 412)
                    log.warn("Object {} was not deleted because it has been modified since it was copied to target.", identifier);
                else throw e;
            }
            return null;
        }, OPERATION_DELETE_OBJECT, object, identifier);
    }

    // COMMON S3 CALLS

    private S3ObjectMetadata getS3Metadata(final String key, final String versionId) {
        return operationWrapper(() -> s3.getObjectMetadata(new GetObjectMetadataRequest(config.getBucketName(), key).withVersionId(versionId)),
                OPERATION_HEAD_OBJECT, null, key);
    }

    private AccessControlList getS3Acl(final String key, final String versionId) {
        return operationWrapper(() -> s3.getObjectAcl(new GetObjectAclRequest(config.getBucketName(), key).withVersionId(versionId)),
                OPERATION_GET_ACL, null, key);
    }

    private InputStream getS3DataStream(final String key, final String versionId) {
        return operationWrapper(() -> {
            GetObjectRequest request = new GetObjectRequest(config.getBucketName(), key).withVersionId(versionId);
            return s3.getObject(request, InputStream.class).getObject();
        }, OPERATION_OPEN_DATA_STREAM, null, key);
    }

    private List<AbstractVersion> getS3Versions(final String key) {
        List<AbstractVersion> versions = new ArrayList<>();

        ListVersionsResult listing = null;
        do {
            final ListVersionsResult fListing = listing;
            listing = operationWrapper(() -> {
                if (fListing == null) {
                    return s3.listVersions(new ListVersionsRequest(config.getBucketName()).withPrefix(key).withDelimiter("/"));
                } else {
                    return s3.listMoreVersions(fListing);
                }
            }, OPERATION_LIST_VERSIONS, null, key);
            listing.setMaxKeys(1000); // Google Storage compatibility

            for (final AbstractVersion version : listing.getVersions()) {
                if (version.getKey().equals(key)) versions.add(version);
            }
        } while (listing.isTruncated());

        return versions;
    }

    // READ TRANSLATION METHODS

    private ObjectMetadata syncMetaFromS3Meta(S3ObjectMetadata s3meta) {
        ObjectMetadata meta = new ObjectMetadata();

        meta.setDirectory(isDirectoryPlaceholder(s3meta.getContentType(), s3meta.getContentLength()));
        meta.setCacheControl(s3meta.getCacheControl());
        meta.setContentDisposition(s3meta.getContentDisposition());
        meta.setContentEncoding(s3meta.getContentEncoding());
        if (s3meta.getContentMd5() != null) {
            // Content-MD5 should be in base64
            meta.setChecksum(Checksum.fromBase64("MD5", s3meta.getContentMd5()));
        } else if (s3meta.getETag() != null && !s3meta.getETag().contains("-")) {
            // ETag is hex
            meta.setChecksum(Checksum.fromHex("MD5", s3meta.getETag()));
        }
        if (s3meta.getETag() != null) meta.setHttpEtag(s3meta.getETag());
        meta.setContentType(s3meta.getContentType());
        meta.setHttpExpires(s3meta.getHttpExpires());
        meta.setExpirationDate(s3meta.getExpirationDate());
        meta.setModificationTime(s3meta.getLastModified());
        meta.setContentLength(s3meta.getContentLength());
        meta.setUserMetadata(toMetaMap(s3meta.getUserMetadata()));
        if (options.isSyncRetentionExpiration() && s3meta.getRetentionPeriod() != null) {
            log.debug("Source retention period: {}", s3meta.getRetentionPeriod());
            Date retentionEndDate = new Date(s3meta.getLastModified().getTime() + s3meta.getRetentionPeriod() * 1000);
            meta.setRetentionEndDate(retentionEndDate);
        }

        return meta;
    }

    private ObjectAcl syncAclFromS3Acl(AccessControlList s3Acl) {
        ObjectAcl syncAcl = new ObjectAcl();
        syncAcl.setOwner(s3Acl.getOwner().getId());
        for (Grant grant : s3Acl.getGrants()) {
            AbstractGrantee grantee = grant.getGrantee();
            if (grantee instanceof Group)
                syncAcl.addGroupGrant(((Group) grantee).getUri(), grant.getPermission().toString());
            else if (grantee instanceof CanonicalUser)
                syncAcl.addUserGrant(((CanonicalUser) grantee).getId(), grant.getPermission().toString());
        }
        return syncAcl;
    }

    // WRITE TRANSLATION METHODS

    private AccessControlList s3AclFromSyncAcl(ObjectAcl syncAcl, boolean ignoreInvalid) {
        AccessControlList s3Acl = new AccessControlList();

        s3Acl.setOwner(new CanonicalUser(syncAcl.getOwner(), syncAcl.getOwner()));

        for (String user : syncAcl.getUserGrants().keySet()) {
            AbstractGrantee grantee = new CanonicalUser(user, user);
            for (String permission : syncAcl.getUserGrants().get(user)) {
                Permission perm = getS3Permission(permission, ignoreInvalid);
                if (perm != null) s3Acl.addGrants(new Grant(grantee, perm));
            }
        }

        for (String group : syncAcl.getGroupGrants().keySet()) {
            AbstractGrantee grantee = new Group(group);
            for (String permission : syncAcl.getGroupGrants().get(group)) {
                Permission perm = getS3Permission(permission, ignoreInvalid);
                if (perm != null) s3Acl.addGrants(new Grant(grantee, perm));
            }
        }

        return s3Acl;
    }

    private Permission getS3Permission(String permission, boolean ignoreInvalid) {
        Permission s3Perm = null;
        try {
            s3Perm = Permission.valueOf(permission);
        } catch (IllegalArgumentException e) {
            if (ignoreInvalid)
                log.warn("{} is not a valid S3 permission", permission);
            else
                throw new RuntimeException(permission + " is not a valid S3 permission");
        }
        return s3Perm;
    }

    private S3ObjectMetadata s3MetaFromSyncMeta(ObjectMetadata syncMeta) {
        S3ObjectMetadata om = new S3ObjectMetadata();
        if (syncMeta.getCacheControl() != null) om.setCacheControl(syncMeta.getCacheControl());
        if (syncMeta.getContentDisposition() != null) om.setContentDisposition(syncMeta.getContentDisposition());
        if (syncMeta.getContentEncoding() != null) om.setContentEncoding(syncMeta.getContentEncoding());
        om.setContentLength(syncMeta.getContentLength());
        if (syncMeta.getChecksum() != null && syncMeta.getChecksum().getAlgorithm().equals("MD5"))
            om.setContentMd5(syncMeta.getChecksum().getBase64Value());
        // handle invalid content-type
        if (syncMeta.getContentType() != null) {
            try {
                if (config.isResetInvalidContentType()) MediaType.valueOf(syncMeta.getContentType());
                om.setContentType(syncMeta.getContentType());
            } catch (IllegalArgumentException e) {
                log.info("Object has Invalid content-type [{}]; resetting to default", syncMeta.getContentType());
            }
        }
        if (syncMeta.getHttpExpires() != null) om.setHttpExpires(syncMeta.getHttpExpires());
        om.setUserMetadata(formatUserMetadata(syncMeta));
        if (syncMeta.getModificationTime() != null) om.setLastModified(syncMeta.getModificationTime());
        if (options.isSyncRetentionExpiration() && syncMeta.getRetentionEndDate() != null) {
            long retentionPeriod = TimeUnit.MILLISECONDS.toSeconds(syncMeta.getRetentionEndDate().getTime() - System.currentTimeMillis());
            log.debug("Target calculated retention period: {}", retentionPeriod);
            if (retentionPeriod < 0L) {
                log.debug("Retention period expired, reset target retention period to 0.");
                retentionPeriod = 0L;
            }
            om.setRetentionPeriod(retentionPeriod);
        }
        if (config.isStoreSourceObjectCopyMarkers()) {
            om.addUserMetadata(AbstractS3Storage.UMD_KEY_SOURCE_MTIME, String.valueOf(syncMeta.getModificationTime().getTime()));
            om.addUserMetadata(AbstractS3Storage.UMD_KEY_SOURCE_ETAG, syncMeta.getHttpEtag());
        }

        return om;
    }

    private void validateBucketWriteAccess(String bucketName) {
        try {
            String versionInfo = s3.listDataNodes().getVersionInfo();
            if (versionInfo.compareTo("3.7") > 0) {
                String key = AwsS3Config.TEST_OBJECT_PREFIX + UUID.randomUUID();
                PutObjectRequest request = new PutObjectRequest(bucketName, key, "").withIfMatch("00000000000000000000000000000000");
                try {
                    // Test write permission with If-Match pre-condition to avoid overwriting existing objects.
                    s3.putObject(request);
                    log.error(key + " has been created during bucket write access validation, needs to be removed by manual. " +
                            "The file should not be created unless If-Match pre-condition does not work.");
                } catch (S3Exception e) {
                    if (e.getHttpCode() == 412 && e.getErrorCode().equals("PreconditionFailed")) {
                        log.debug("Write Access Check is passed on bucket " + bucketName);
                    } else if (e.getHttpCode() == 403) {
                        throw new S3ConfigurationException(Error.ERROR_BUCKET_ACCESS_WRITE, e);
                    } else
                        throw new S3ConfigurationException(Error.ERROR_BUCKET_ACCESS_UNKNOWN, e);
                }
            } else {
                log.warn("Skip bucket write access validation because ECS version is < 3.7.1: {}", versionInfo);
            }
        } catch (S3Exception e) {
            log.warn("Skip bucket write access validation because S3 Storage Server may not be ECS: " + e);
        }
    }

    private class PrefixIterator extends ReadOnlyIterator<ObjectSummary> {
        private final String prefix;
        private ListObjectsResult listing;
        private Iterator<S3Object> objectIterator;

        PrefixIterator(String prefix) {
            this.prefix = prefix;
        }

        @Override
        protected ObjectSummary getNextObject() {
            if (listing == null || (!objectIterator.hasNext() && listing.isTruncated())) {
                getNextBatch();
            }

            if (objectIterator.hasNext()) {
                S3Object object = objectIterator.next();
                return new ObjectSummary(object.getKey(), false, object.getSize());
            }

            // list is not truncated and iterators are finished; no more objects
            return null;
        }

        private void getNextBatch() {
            if (listing == null) {
                listing = time(() -> {
                    ListObjectsRequest request = new ListObjectsRequest(config.getBucketName());
                    request.setPrefix("".equals(prefix) ? null : prefix);
                    if (config.isUrlEncodeKeys()) request.setEncodingType(EncodingType.url);
                    return s3.listObjects(request);
                }, OPERATION_LIST_OBJECTS);
            } else {
                log.info("getting next page of objects [prefix: {}, marker: {}, nextMarker: {}, encodingType: {}, maxKeys: {}]",
                        listing.getPrefix(), listing.getMarker(), listing.getNextMarker(), listing.getEncodingType(), listing.getMaxKeys());
                listing = time(() -> s3.listMoreObjects(listing), OPERATION_LIST_OBJECTS);
            }
            objectIterator = listing.getObjects().iterator();
        }
    }

    private class DeletedObjectIterator extends ReadOnlyIterator<ObjectSummary> {
        private final String prefix;
        private ListVersionsResult versionListing;
        private Iterator<AbstractVersion> versionIterator;

        DeletedObjectIterator(String prefix) {
            this.prefix = prefix;
        }

        @Override
        protected ObjectSummary getNextObject() {
            while (true) {
                AbstractVersion version = getNextVersion();

                if (version == null) return null;

                if (version.isLatest() && version instanceof DeleteMarker)
                    return new ObjectSummary(version.getKey(), false, 0);
            }
        }

        private AbstractVersion getNextVersion() {
            // look for deleted objects in versioned bucket
            if (versionListing == null || (!versionIterator.hasNext() && versionListing.isTruncated())) {
                getNextVersionBatch();
            }

            if (versionIterator.hasNext()) {
                return versionIterator.next();
            }

            // no more versions
            return null;
        }

        private void getNextVersionBatch() {
            if (versionListing == null) {
                versionListing = time(() -> {
                    ListVersionsRequest request = new ListVersionsRequest(config.getBucketName());
                    request.setPrefix("".equals(prefix) ? null : prefix);
                    if (config.isUrlEncodeKeys()) request.setEncodingType(EncodingType.url);
                    return s3.listVersions(request);
                }, OPERATION_LIST_VERSIONS);
            } else {
                versionListing = time(() -> s3.listMoreVersions(versionListing), OPERATION_LIST_VERSIONS);
            }
            versionIterator = versionListing.getVersions().iterator();
        }
    }
}

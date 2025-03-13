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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.conn.ssl.SdkTLSSocketFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.emc.ecs.sync.NonRetriableException;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.AwsS3Config;
import com.emc.ecs.sync.config.storage.S3ConfigurationException;
import com.emc.ecs.sync.config.storage.S3ConfigurationException.Error;
import com.emc.ecs.sync.config.storage.S3WriteAccessException;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.Checksum;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.ObjectNotFoundException;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.*;
import com.emc.object.s3.lfu.LargeFileMultipartSource;
import com.emc.object.s3.lfu.LargeFileUpload;
import com.emc.object.s3.lfu.LargeFileUploaderResumeContext;
import com.emc.object.util.ProgressInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

public class AwsS3Storage extends AbstractS3Storage<AwsS3Config> implements OptionChangeListener {
    private static final Logger log = LoggerFactory.getLogger(AwsS3Storage.class);

    private static final int MAX_PUT_SIZE_MB = 5 * 1024; // 5GB
    private static final int MIN_PART_SIZE_MB = 5;
    public static final long MAX_OBJECT_SIZE = 5L * 1024 * 1024 * 1024 * 1024; // 5TB

    // timed operations
    public static final String OPERATION_LIST_OBJECTS = "AwsS3ListObjects";
    public static final String OPERATION_LIST_VERSIONS = "AwsS3ListVersions";
    public static final String OPERATION_HEAD_OBJECT = "AwsS3HeadObject";
    public static final String OPERATION_GET_ACL = "AwsS3GetAcl";
    public static final String OPERATION_OPEN_DATA_STREAM = "AwsS3OpenDataStream";
    public static final String OPERATION_WRITE_OBJECT = "AwsS3WriteObject";
    public static final String OPERATION_DELETE_VERSIONS = "AwsS3DeleteVersions";
    public static final String OPERATION_DELETE_OBJECT = "AwsS3DeleteObject";
    public static final String OPERATION_UPDATE_METADATA = "AwsS3UpdateMetadata";

    public static final String ERROR_CODE_OBJECT_TOO_LARGE = "AwsS3ObjectTooLarge";

    private AmazonS3 s3;
    private List<Pattern> excludedKeyPatterns;
    private EnhancedThreadPoolExecutor mpuThreadPool;

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        // if any of these are present, we need both the access-key and secret-key
        if (config.getSessionToken() != null
                || config.getAccessKey() != null
                || config.getSecretKey() != null) {
            if (config.getAccessKey() == null) throw new ConfigurationException("accessKey is required");
            if (config.getSecretKey() == null) throw new ConfigurationException("secretKey is required");
        }
        // must specify one of [default-credentials, profile name, or access-key] as auth type
        if (!(config.getUseDefaultCredentialsProvider()
                || config.getProfile() != null
                || config.getAccessKey() != null)) {
            throw new ConfigurationException("must provide one of [useDefaultCredentialsProvider, profile, accessKey+secretKey] as authentication method to use");
        }

        if (config.getBucketName() == null) throw new ConfigurationException("bucketName is required");
        if (!config.getBucketName().matches("[A-Za-z0-9._-]+")) {
            throw new ConfigurationException(config.getBucketName() + " is not a valid bucket name");
        }

        AwsS3CredentialsProviderChain.Builder providerChainBuilder = AwsS3CredentialsProviderChain.builder();

        // if session-token is present, use session credentials as auth
        if (config.getSessionToken() != null && config.getSessionToken().length() > 0) {
            providerChainBuilder.addCredentials(
                    new BasicSessionCredentials(config.getAccessKey(), config.getSecretKey(), config.getSessionToken()));

            // otherwise, if access-key is present, use basic credentials
        } else if (config.getAccessKey() != null && config.getAccessKey().length() > 0) {
            providerChainBuilder.addCredentials(new BasicAWSCredentials(config.getAccessKey(), config.getSecretKey()));
        }

        if (config.getProfile() != null && config.getProfile().length() > 0) {
            providerChainBuilder.addProfileCredentialsProvider(config.getProfile());
        }

        if (config.getUseDefaultCredentialsProvider()) {
            providerChainBuilder.addDefaultProviders();
        }

        ClientConfiguration cc = new ClientConfiguration();

        // set max connections to a high number to allow for increased thread count, since we cannot change this later
        // by default, idle connections in the pool will be closed by a reaper thread after they are unused for 60 seconds
        cc.setMaxConnections(500);

        if (config.getProtocol() != null)
            cc.setProtocol(Protocol.valueOf(config.getProtocol().toString().toUpperCase()));

        if (config.isLegacySignatures()) cc.setSignerOverride("S3SignerType");

        if (config.getSocketTimeoutMs() >= 0) cc.setSocketTimeout(config.getSocketTimeoutMs());

        if (config.getProtocol().equals(com.emc.ecs.sync.config.Protocol.https) && config.getBase64TlsCertificate() != null) {
            try {
                SSLContext sslContext = SSLUtil.getSSLContext(config.getBase64TlsCertificate());
                SdkTLSSocketFactory sslSocketFactory = new SdkTLSSocketFactory(sslContext, null);
                cc.getApacheHttpClientConfig().setSslSocketFactory(sslSocketFactory);
            } catch (Exception e) {
                throw new S3ConfigurationException(Error.ERROR_INVALID_TLS_CERTIFICATE, e);
            }
        }

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withCredentials(providerChainBuilder.build())
                .withClientConfiguration(cc);

        if (config.getHost() != null) {
            String endpoint = "";
            if (config.getProtocol() != null) endpoint += config.getProtocol() + "://";
            endpoint += config.getHost();
            if (config.getPort() > 0) endpoint += ":" + config.getPort();
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, config.getRegion()));
        } else if (config.getRegion() != null) {
            builder.withRegion(config.getRegion());
        }

        if (config.isDisableVHosts()) {
            log.info("The use of virtual hosted buckets has been DISABLED.  Path style buckets will be used.");
            builder.withPathStyleAccessEnabled(true);
        }

        s3 = builder.build();

        boolean bucketExists;
        try {
            bucketExists = s3.doesBucketExistV2(config.getBucketName());
        } catch (SdkClientException e) {
            // Note: the above call should *not* throw an exception unless something is wrong in the environment
            // (network/service issue)
            throw new ConfigurationException("cannot determine if " + getRole() + " bucket exists", e);
        }

        boolean bucketHasVersions = false;
        if (bucketExists && config.isIncludeVersions()) {
            // check if versioning has ever been enabled on the bucket (versions will not be collected unless required)
            BucketVersioningConfiguration versioningConfig = s3.getBucketVersioningConfiguration(config.getBucketName());
            List<String> versionedStates = Arrays.asList(BucketVersioningConfiguration.ENABLED, BucketVersioningConfiguration.SUSPENDED);
            bucketHasVersions = versionedStates.contains(versioningConfig.getStatus());
        }

        if (config.getKeyPrefix() == null) config.setKeyPrefix(""); // make sure keyPrefix isn't null

        if (source == this) {
            if (config.getExcludedKeys() != null) {
                excludedKeyPatterns = new ArrayList<>();
                for (String pattern : config.getExcludedKeys()) {
                    excludedKeyPatterns.add(Pattern.compile(pattern));
                }
            }
        }

        if (target == this) {
            // create bucket if it doesn't exist
            if (!bucketExists && config.isCreateBucket()) {
                s3.createBucket(config.getBucketName());
                bucketExists = true;
                if (config.isIncludeVersions()) {
                    s3.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(config.getBucketName(),
                            new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
                    bucketHasVersions = true;
                }
            }

            // make sure MPU settings are valid
            if (config.getMpuThresholdMb() > MAX_PUT_SIZE_MB) {
                log.warn("{}MB is above the maximum PUT size of {}MB. the maximum will be used instead",
                        config.getMpuThresholdMb(), MAX_PUT_SIZE_MB);
                config.setMpuThresholdMb(MAX_PUT_SIZE_MB);
            }
            if (config.getMpuPartSizeMb() < MIN_PART_SIZE_MB) {
                log.warn("{}MB is below the minimum MPU part size of {}MB. the minimum will be used instead",
                        config.getMpuPartSizeMb(), MIN_PART_SIZE_MB);
                config.setMpuPartSizeMb(MIN_PART_SIZE_MB);
            }
        }

        // make sure bucket exists
        if (!bucketExists)
            throw new ConfigurationException("The bucket " + config.getBucketName() + " does not exist.");

        // if syncing versions, make sure plugins support it and bucket has versioning enabled
        if (config.isIncludeVersions()) {
            if (!(source instanceof AbstractS3Storage && target instanceof AbstractS3Storage))
                throw new ConfigurationException("Version migration is only supported between two S3 plugins");

            if (!bucketHasVersions)
                throw new ConfigurationException("The specified bucket does not have versioning enabled.");
        }

        if (target == this && config.isWriteTestObject())
            writeTestObject(config.getBucketName());

        mpuThreadPool = new EnhancedThreadPoolExecutor(options.getThreadCount(), new LinkedBlockingDeque<>(100), "mpu-pool");
    }

    @Override
    public void optionsChanged(SyncOptions options) {
        // if the thread-count changes, modify the MPU thread pool accordingly
        mpuThreadPool.resizeThreadPool(options.getThreadCount());
        // NOTE: there is no way to change maxConnections in the S3 client, so we set that to a high number in configure()
    }

    @Override
    public void close() {
        try {
            if (mpuThreadPool != null) mpuThreadPool.shutdown();
        } catch (Exception e) {
            log.warn("could not shutdown MPU thread pool", e);
        }
        try {
            if (s3 != null) s3.shutdown();
        } catch (Exception e) {
            log.warn("could not shutdown S3 client", e);
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
    protected ObjectSummary createSummary(final String identifier) {
        ObjectMetadata objectMetadata = getS3Metadata(identifier, null);
        return new ObjectSummary(identifier, false, objectMetadata.getContentLength());
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
    SyncObject loadObject(final String key, final String versionId) throws ObjectNotFoundException {
        // load metadata
        com.emc.ecs.sync.model.ObjectMetadata metadata;
        try {
            metadata = syncMetaFromS3Meta(getS3Metadata(key, versionId));
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
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

        return object;
    }

    @Override
    List<S3ObjectVersion> loadVersions(final String key) {
        List<S3ObjectVersion> versions = new ArrayList<>();

        boolean directory = false; // delete markers won't have any metadata, so keep track of directory status
        for (S3VersionSummary summary : getS3Versions(key)) {
            S3ObjectVersion version;
            if (summary.isDeleteMarker()) {
                version = new S3ObjectVersion(this, getRelativePath(key, directory),
                        new com.emc.ecs.sync.model.ObjectMetadata().withModificationTime(summary.getLastModified())
                                .withContentLength(0).withDirectory(directory));
            } else {
                version = (S3ObjectVersion) loadObject(key, summary.getVersionId());
                // interestingly, S3 list results will include milliseconds in the mtime, but the actual object
                // Last-Modified header will be truncated.. so we'll replace that here for an accurate sorting
                version.getMetadata().setModificationTime(summary.getLastModified());
                directory = version.getMetadata().isDirectory();
            }
            version.setVersionId(summary.getVersionId());
            version.setETag(summary.getETag());
            version.setLatest(summary.isLatest());
            version.setDeleteMarker(summary.isDeleteMarker());
            versions.add(version);
        }

        versions.sort(new S3VersionComparator());

        return versions;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateObject(final String identifier, SyncObject object) {
        try {
            // skip the root of the bucket since it obviously exists
            if ("".equals(config.getKeyPrefix() + object.getRelativePath())) {
                log.debug("Target is bucket root; skipping");
                return;
            }

            // check early on to see if we should ignore directories
            if (!config.isPreserveDirectories() && object.getMetadata().isDirectory()) {
                log.debug("Source is directory and preserveDirectories is false; skipping");
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
                            log.info("Source and target versions are the same. Skipping {}", object.getRelativePath());
                            return;
                        }
                    }
                }

                // something's off; must delete all versions of the object
                if (replaceVersions) {
                    log.info("[{}]: version history differs between source and target; re-placing target version history with that from source.",
                            object.getRelativePath());

                    // collect versions in target
                    final List<DeleteObjectsRequest.KeyVersion> deleteVersions = new ArrayList<>();
                    while (targetVersions.hasNext()) targetVersions.next(); // move cursor to end
                    while (targetVersions.hasPrevious()) { // go in reverse order
                        S3ObjectVersion version = targetVersions.previous();
                        deleteVersions.add(new DeleteObjectsRequest.KeyVersion(identifier, version.getVersionId()));
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
                    log.debug("[{}]: updating metadata after sync as required", object.getRelativePath());
                    final CopyObjectRequest cReq = new CopyObjectRequest(config.getBucketName(), identifier, config.getBucketName(), identifier);
                    ObjectMetadata om = s3MetaFromSyncMeta(object.getMetadata());
                    if (config.isSseS3Enabled()) om.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                    cReq.setNewObjectMetadata(om);
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
    void putObject(SyncObject obj, String targetKey) {
        ObjectMetadata om;
        if (options.isSyncMetadata()) {
            om = s3MetaFromSyncMeta(obj.getMetadata());
        } else {
            om = new ObjectMetadata();
            om.setContentLength(obj.getMetadata().getContentLength());
        }

        if (obj.getMetadata().isDirectory()) om.setContentType(TYPE_DIRECTORY);

        if (config.isSseS3Enabled()) om.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

        if (obj.getProperty(PROP_SOURCE_ETAG_MATCHES) != null && (Boolean) obj.getProperty(PROP_SOURCE_ETAG_MATCHES)) {
            // target data already matches source, so we only need to update metadata
            // the ECS LFU doesn't support MPU copy, so we have to use the AWS TM
            TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3).build();
            CopyObjectRequest copyRequest = new CopyObjectRequest(config.getBucketName(), targetKey, config.getBucketName(), targetKey)
                    .withNewObjectMetadata(om);

            if (options.isSyncAcl())
                copyRequest.setAccessControlList(s3AclFromSyncAcl(obj.getAcl(), options.isIgnoreInvalidAcls()));

            operationWrapper((Function<Void>) () -> {
                try {
                    // NOTE: default MPU copy threshold is 5GB (max object size), which is what we want
                    transferManager.copy(copyRequest).waitForCompletion();
                    return null;
                } catch (InterruptedException e) {
                    // this is most likely due to the job getting terminated early, or some unknown condition we can't handle
                    throw new RuntimeException(e);
                }
            }, OPERATION_UPDATE_METADATA, obj, targetKey);

        } else {

            AwsS3LargeFileUploader uploader;

            // Note: obj.getAcl() and obj.getDataStream() might both make a call to the source storage system.
            //       However, obj.getDataStream() (if called) will hold a connection until after we write to the target.
            //       To avoid consuming 2 simultaneous connections to the source system, getAcl() should come first
            AccessControlList acl = null;
            if (options.isSyncAcl()) acl = s3AclFromSyncAcl(obj.getAcl(), options.isIgnoreInvalidAcls());

            // TODO: we should not reference another plugin from this one - where should this constant live??
            File file = (File) obj.getProperty("filesystem.file");
            if (obj.getMetadata().isDirectory()) {
                uploader = new AwsS3LargeFileUploader(s3, config.getBucketName(), targetKey, new ByteArrayInputStream(new byte[0]), 0);
            } else if (file != null) {
                // we can read file parts in parallel
                uploader = new AwsS3LargeFileUploader(s3, config.getBucketName(), targetKey, file);
                // because we are bypassing the source data stream, we need to make
                // sure to update the source-read and target-write windows, as well as the object's bytes-read
                // TODO: apply bandwidth throttle here
                uploader.setProgressListener(new ByteTransferListener(obj));
            } else if (obj.getProperty(AbstractS3Storage.PROP_MULTIPART_SOURCE) != null) {
                // our source object supports parallel streams
                // TODO: need to generalize a parallel stream source as a 1st-class concept in ecs-sync
                //       i.e. maybe embed logic in SyncObject so that streams are wrapped properly and checksum
                //       calculation is automatic
                uploader = new AwsS3LargeFileUploader(s3, config.getBucketName(), targetKey,
                        (LargeFileMultipartSource) obj.getProperty(AbstractS3Storage.PROP_MULTIPART_SOURCE));
                // because we are bypassing the source data stream, we need to make
                // sure to update the source-read and target-write windows, as well as the object's bytes-read
                uploader.setProgressListener(new ByteTransferListener(obj));
            } else {
                InputStream dataStream = obj.getDataStream();
                if (options.isMonitorPerformance())
                    dataStream = new ProgressInputStream(dataStream, new PerformanceListener(getWriteWindow()));
                uploader = new AwsS3LargeFileUploader(s3, config.getBucketName(), targetKey, dataStream, obj.getMetadata().getContentLength());
                uploader.setCloseStream(true);
            }
            uploader.withPartSize((long) config.getMpuPartSizeMb() * 1024 * 1024).withMpuThreshold((long) config.getMpuThresholdMb() * 1024 * 1024);
            uploader.setAwsS3ObjectMetadata(om);

            if (options.isSyncAcl()) uploader.setAwsS3Acl(acl);

            // if resume-mpu is enabled, try to find an existing uploadId to resume
            if (config.isMpuResumeEnabled()) {
                uploader.setAbortMpuOnFailure(false); // see additional MPU abort logic in the catch block below
                if (obj.getMetadata().getContentLength() > (long) config.getMpuThresholdMb() * 1024 * 1024) {
                    String uploadId = getLatestMultipartUploadId(targetKey, obj.getMetadata().getModificationTime());
                    if (uploadId != null) {
                        uploader.setResumeContext(new LargeFileUploaderResumeContext().withUploadId(uploadId));
                    }
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
                                    log.debug("doSinglePut: ResumeContext {}", uploader.getResumeContext());
                                    // doSinglePut(size < mpuThreshold = 512MB) will not set ResumeContext when finished
                                    if (uploader.getResumeContext() == null) {
                                        log.info("Object(size < mpuThreshold) has been uploaded completely, key: {}", uploader.getKey());
                                        break;
                                    }
                                    // if this is an interrupted MPU, it should not be a "success" in the stats
                                    // since there is no stat for partially copied objects, it will just be an error
                                    if (uploader.getResumeContext() != null && uploader.getResumeContext().getUploadId() != null)
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
                }, OPERATION_WRITE_OBJECT, obj, targetKey);
                log.debug("Wrote {}; etag: {}", targetKey, uploader.getETag());
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

    // any exceptions the operation listener must see should be thrown here
    @Override
    protected <T> T operationWrapper(Function<T> function, String operationName, SyncObject syncObject, String identifier) {
        return super.operationWrapper(() -> {
            // AWS has a 5TB object size limit
            if (OPERATION_WRITE_OBJECT.equals(operationName) && syncObject.getMetadata().getContentLength() > MAX_OBJECT_SIZE) {
                throw new NonRetriableException(ERROR_CODE_OBJECT_TOO_LARGE,
                        String.format("The object %s (%d bytes) exceeds the maximum object size of %d",
                                syncObject.getRelativePath(), syncObject.getMetadata().getContentLength(), MAX_OBJECT_SIZE));
            }
            return function.call();
        }, operationName, syncObject, identifier);
    }

    /*
     * returns the latest (most recently initiated) MPU for the configured bucket/key that was initiated after
     * initiatedAfter, or null if none is found.
     */
    private String getLatestMultipartUploadId(String objectKey, Date initiatedAfter) {
        MultipartUpload latestUpload = null;
        try {
            ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(config.getBucketName()).withPrefix(objectKey);
            MultipartUploadListing multipartUploadListing = null;
            do {
                if (multipartUploadListing == null) {
                    multipartUploadListing = s3.listMultipartUploads(request);
                } else {
                    multipartUploadListing = s3.listMultipartUploads(request.withKeyMarker(multipartUploadListing.getNextKeyMarker()).withUploadIdMarker(multipartUploadListing.getNextUploadIdMarker()));
                }
                for (MultipartUpload upload : multipartUploadListing.getMultipartUploads()) {
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
            } while (multipartUploadListing.isTruncated());
        } catch (Exception e) {
            log.warn("Error retrieving MPU uploads in target", e);
            latestUpload = null;
        }
        if (latestUpload == null) return null;
        return latestUpload.getUploadId();
    }

    //TODO: make sure that the source object has not been modified since it was copied to the target, before deleting
    @Override
    public void delete(final String identifier, SyncObject object) {
        operationWrapper((Function<Void>) () -> {
            s3.deleteObject(config.getBucketName(), identifier);
            return null;
        }, OPERATION_DELETE_OBJECT, object, identifier);
    }

    // COMMON S3 CALLS

    private ObjectMetadata getS3Metadata(final String key, final String versionId) {
        return operationWrapper(() -> {
            GetObjectMetadataRequest request = new GetObjectMetadataRequest(config.getBucketName(), key, versionId);
            return s3.getObjectMetadata(request);
        }, OPERATION_HEAD_OBJECT, null, key);
    }

    private AccessControlList getS3Acl(final String key, final String versionId) {
        return operationWrapper(() -> {
            if (versionId == null) return s3.getObjectAcl(config.getBucketName(), key);
            else return s3.getObjectAcl(config.getBucketName(), key, versionId);
        }, OPERATION_GET_ACL, null, key);
    }

    private InputStream getS3DataStream(final String key, final String versionId) {
        return operationWrapper((Function<InputStream>) () -> {
            GetObjectRequest request = new GetObjectRequest(config.getBucketName(), key, versionId);
            return s3.getObject(request).getObjectContent();
        }, OPERATION_OPEN_DATA_STREAM, null, key);
    }

    private List<S3VersionSummary> getS3Versions(final String key) {
        List<S3VersionSummary> versions = new ArrayList<>();

        VersionListing listing = null;
        do {
            final VersionListing fListing = listing;
            listing = operationWrapper(() -> {
                if (fListing == null) {
                    ListVersionsRequest request = new ListVersionsRequest().withBucketName(config.getBucketName());
                    request.withPrefix(key).withDelimiter("/");
                    // Note: AWS SDK will always set encoding-type=url, but will only decode automatically if we
                    // leave the value null.. manually setting it here allows us to disable automatic decoding,
                    // but if the storage actually encodes the keys, they will be corrupted.. only do this if the
                    // storage does *not* respect the encoding-type parameter!
                    if (!config.isUrlDecodeKeys()) request.setEncodingType(Constants.URL_ENCODING);
                    return s3.listVersions(request);
                } else {
                    return s3.listNextBatchOfVersions(fListing);
                }
            }, OPERATION_LIST_VERSIONS, null, key);
            listing.setMaxKeys(1000); // Google Storage compatibility

            for (final S3VersionSummary summary : listing.getVersionSummaries()) {
                if (summary.getKey().equals(key)) versions.add(summary);
            }
        } while (listing.isTruncated());

        return versions;
    }

    // READ TRANSLATION METHODS

    private com.emc.ecs.sync.model.ObjectMetadata syncMetaFromS3Meta(ObjectMetadata s3meta) {
        com.emc.ecs.sync.model.ObjectMetadata meta = new com.emc.ecs.sync.model.ObjectMetadata();

        meta.setDirectory(isDirectoryPlaceholder(s3meta.getContentType(), s3meta.getContentLength()));
        meta.setCacheControl(s3meta.getCacheControl());
        meta.setContentDisposition(s3meta.getContentDisposition());
        meta.setContentEncoding(s3meta.getContentEncoding());
        if (s3meta.getContentMD5() != null) {
            // Content-MD5 should be in base64
            meta.setChecksum(Checksum.fromBase64("MD5", s3meta.getContentMD5()));
        } else if (s3meta.getETag() != null && !s3meta.getETag().contains("-")) {
            // ETag is hex
            meta.setChecksum(Checksum.fromHex("MD5", s3meta.getETag()));
        }
        meta.setContentType(s3meta.getContentType());
        meta.setHttpExpires(s3meta.getHttpExpiresDate());
        meta.setExpirationDate(s3meta.getExpirationTime());
        meta.setModificationTime(s3meta.getLastModified());
        meta.setContentLength(s3meta.getContentLength());
        meta.setUserMetadata(toMetaMap(s3meta.getUserMetadata()));

        return meta;
    }

    private ObjectAcl syncAclFromS3Acl(AccessControlList s3Acl) {
        ObjectAcl syncAcl = new ObjectAcl();
        syncAcl.setOwner(s3Acl.getOwner().getId());
        for (Grant grant : s3Acl.getGrantsAsList()) {
            Grantee grantee = grant.getGrantee();
            if (grantee instanceof GroupGrantee || grantee.getTypeIdentifier().equals(ACL_GROUP_TYPE))
                syncAcl.addGroupGrant(grantee.getIdentifier(), grant.getPermission().toString());
            else if (grantee instanceof CanonicalGrantee || grantee.getTypeIdentifier().equals(ACL_CANONICAL_USER_TYPE))
                syncAcl.addUserGrant(grantee.getIdentifier(), grant.getPermission().toString());
        }
        return syncAcl;
    }

    // WRITE TRANSLATION METHODS

    private AccessControlList s3AclFromSyncAcl(ObjectAcl syncAcl, boolean ignoreInvalid) {
        AccessControlList s3Acl = new AccessControlList();

        s3Acl.setOwner(new Owner(syncAcl.getOwner(), syncAcl.getOwner()));

        for (String user : syncAcl.getUserGrants().keySet()) {
            Grantee grantee = new CanonicalGrantee(user);
            for (String permission : syncAcl.getUserGrants().get(user)) {
                Permission perm = getS3Permission(permission, ignoreInvalid);
                if (perm != null) s3Acl.grantPermission(grantee, perm);
            }
        }

        for (String group : syncAcl.getGroupGrants().keySet()) {
            Grantee grantee = GroupGrantee.parseGroupGrantee(group);
            if (grantee == null) {
                if (ignoreInvalid)
                    log.warn("{} is not a valid S3 group", group);
                else
                    throw new RuntimeException(group + " is not a valid S3 group");
            }
            for (String permission : syncAcl.getGroupGrants().get(group)) {
                Permission perm = getS3Permission(permission, ignoreInvalid);
                if (perm != null) s3Acl.grantPermission(grantee, perm);
            }
        }

        return s3Acl;
    }

    private ObjectMetadata s3MetaFromSyncMeta(com.emc.ecs.sync.model.ObjectMetadata syncMeta) {
        com.amazonaws.services.s3.model.ObjectMetadata om = new com.amazonaws.services.s3.model.ObjectMetadata();
        if (syncMeta.getCacheControl() != null) om.setCacheControl(syncMeta.getCacheControl());
        if (syncMeta.getContentDisposition() != null) om.setContentDisposition(syncMeta.getContentDisposition());
        if (syncMeta.getContentEncoding() != null) om.setContentEncoding(syncMeta.getContentEncoding());
        om.setContentLength(syncMeta.getContentLength());
        if (syncMeta.getChecksum() != null && syncMeta.getChecksum().getAlgorithm().equals("MD5"))
            om.setContentMD5(syncMeta.getChecksum().getBase64Value());
        if (syncMeta.getContentType() != null) om.setContentType(syncMeta.getContentType());
        if (syncMeta.getHttpExpires() != null) om.setHttpExpiresDate(syncMeta.getHttpExpires());
        om.setUserMetadata(formatUserMetadata(syncMeta));
        if (syncMeta.getModificationTime() != null) om.setLastModified(syncMeta.getModificationTime());
        if (config.isStoreSourceObjectCopyMarkers()) {
            om.addUserMetadata(AbstractS3Storage.UMD_KEY_SOURCE_MTIME, String.valueOf(syncMeta.getModificationTime().getTime()));
            if (syncMeta.getHttpEtag() != null)
                om.addUserMetadata(AbstractS3Storage.UMD_KEY_SOURCE_ETAG, syncMeta.getHttpEtag());
        }
        return om;
    }

    private Permission getS3Permission(String permission, boolean ignoreInvalid) {
        Permission s3Perm = Permission.parsePermission(permission);
        if (s3Perm == null) {
            if (ignoreInvalid)
                log.warn("{} is not a valid S3 permission", permission);
            else
                throw new RuntimeException(permission + " is not a valid S3 permission");
        }
        return s3Perm;
    }

    private class PrefixIterator extends ReadOnlyIterator<ObjectSummary> {
        private final String prefix;
        private ObjectListing listing;
        private Iterator<S3ObjectSummary> objectIterator;

        PrefixIterator(String prefix) {
            this.prefix = prefix;
        }

        @Override
        protected ObjectSummary getNextObject() {
            nextObjectLoop:
            while (true) {
                if (listing == null || (!objectIterator.hasNext() && listing.isTruncated())) {
                    getNextBatch();
                }

                if (objectIterator.hasNext()) {
                    S3ObjectSummary summary = objectIterator.next();
                    String key = summary.getKey();

                    // apply exclusion filter
                    if (excludedKeyPatterns != null) {
                        for (Pattern p : excludedKeyPatterns) {
                            if (p.matcher(key).matches()) {
                                log.info("excluding file {}: matches pattern: {}", key, p);
                                continue nextObjectLoop;
                            }
                        }
                    }

                    return new ObjectSummary(key, false, summary.getSize());
                }

                // list is not truncated and iterators are finished; no more objects
                return null;
            }
        }

        private void getNextBatch() {
            if (listing == null) {
                listing = time(() -> {
                    ListObjectsRequest request = new ListObjectsRequest().withBucketName(config.getBucketName());
                    request.setPrefix("".equals(prefix) ? null : prefix);
                    // Note: AWS SDK will always set encoding-type=url, but will only decode automatically if we
                    // leave the value null.. manually setting it here allows us to disable automatic decoding,
                    // but if the storage actually encodes the keys, they will be corrupted.. only do this if the
                    // storage does *not* respect the encoding-type parameter!
                    if (!config.isUrlDecodeKeys()) request.setEncodingType(Constants.URL_ENCODING);
                    return s3.listObjects(request);
                }, OPERATION_LIST_OBJECTS);
            } else {
                listing = time(() -> s3.listNextBatchOfObjects(listing), OPERATION_LIST_OBJECTS);
            }
            listing.setMaxKeys(1000); // Google Storage compatibility
            objectIterator = listing.getObjectSummaries().iterator();
        }
    }

    private class DeletedObjectIterator extends ReadOnlyIterator<ObjectSummary> {
        private final String prefix;
        private VersionListing versionListing;
        private Iterator<S3VersionSummary> versionIterator;

        DeletedObjectIterator(String prefix) {
            this.prefix = prefix;
        }

        @Override
        protected ObjectSummary getNextObject() {
            while (true) {
                S3VersionSummary versionSummary = getNextSummary();

                if (versionSummary == null) return null;

                if (versionSummary.isLatest() && versionSummary.isDeleteMarker()) {
                    String key = versionSummary.getKey();
                    return new ObjectSummary(key, false, versionSummary.getSize());
                }
            }
        }

        private S3VersionSummary getNextSummary() {
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
                    ListVersionsRequest request = new ListVersionsRequest().withBucketName(config.getBucketName());
                    request.setPrefix("".equals(prefix) ? null : prefix);
                    // Note: AWS SDK will always set encoding-type=url, but will only decode automatically if we
                    // leave the value null.. manually setting it here allows us to disable automatic decoding,
                    // but if the storage actually encodes the keys, they will be corrupted.. only do this if the
                    // storage does *not* respect the encoding-type parameter!
                    if (!config.isUrlDecodeKeys()) request.setEncodingType(Constants.URL_ENCODING);
                    return s3.listVersions(request);
                }, OPERATION_LIST_VERSIONS);
            } else {
                versionListing.setMaxKeys(1000); // Google Storage compatibility
                versionListing = time(() -> s3.listNextBatchOfVersions(versionListing), OPERATION_LIST_VERSIONS);
            }
            versionIterator = versionListing.getVersionSummaries().iterator();
        }
    }

    private void writeTestObject(String bucketName) {
        String objectName = AwsS3Config.TEST_OBJECT_PREFIX + UUID.randomUUID();
        try {
            s3.putObject(bucketName, objectName, "");
        } catch (AmazonS3Exception e) {
            log.warn("unable to create test object {}/{}", bucketName, objectName);
            if (e.getStatusCode() == 403) {
                throw new S3WriteAccessException(Error.ERROR_BUCKET_ACCESS_WRITE, objectName, e);
            } else {
                throw new ConfigurationException(e);
            }
        }
        try {
            s3.deleteObject(bucketName, objectName);
        } catch (AmazonS3Exception e) {
            log.warn("unable to delete test object {}/{}", bucketName, objectName);
            if (e.getStatusCode() == 403) {
                throw new S3WriteAccessException(Error.ERROR_BUCKET_ACCESS_DELETE, objectName, e);
            } else {
                throw new ConfigurationException(e);
            }
        }
    }
}

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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.storage.AwsS3Config;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.Checksum;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.ObjectNotFoundException;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.file.AbstractFilesystemStorage;
import com.emc.ecs.sync.util.*;
import com.emc.object.util.ProgressInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

public class AwsS3Storage extends AbstractS3Storage<AwsS3Config> {
    private static final Logger log = LoggerFactory.getLogger(AwsS3Storage.class);

    private static final int MAX_PUT_SIZE_MB = 5 * 1024; // 5GB
    private static final int MIN_PART_SIZE_MB = 5;

    // timed operations
    private static final String OPERATION_LIST_OBJECTS = "AwsS3ListObjects";
    private static final String OPERATION_LIST_VERSIONS = "AwsS3ListVersions";
    private static final String OPERATION_HEAD_OBJECT = "AwsS3HeadObject";
    private static final String OPERATION_GET_ACL = "AwsS3GetAcl";
    private static final String OPERATION_OPEN_DATA_STREAM = "AwsS3OpenDataStream";
    private static final String OPERATION_MPU = "AwsS3MultipartUpload";
    private static final String OPERATION_DELETE_OBJECTS = "AwsS3DeleteObjects";
    private static final String OPERATION_DELETE_OBJECT = "AwsS3DeleteObject";
    private static final String OPERATION_UPDATE_METADATA = "AwsS3UpdateMetadata";

    private AmazonS3 s3;
    private PerformanceWindow sourceReadWindow;
    private List<Pattern> excludedKeyPatterns;

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        if (config.getSessionToken() != null
            || config.getAccessKey() != null
            || config.getSecretKey() != null) {
            Assert.hasText(config.getAccessKey(), "accessKey is required");
            Assert.hasText(config.getSecretKey(), "secretKey is required");
            Assert.isTrue(config.getSessionToken() == null || config.getSessionToken().length() > 0,
                          "sessionToken can not be empty if provided");
        }
        Assert.isTrue(config.getUseDefaultCredentialsProvider()
                          || config.getProfile() != null
                          || config.getAccessKey() != null,
            "must provide authentication method to use");

        Assert.isTrue(config.getProfile() == null || config.getProfile().length() > 0,
            "Profile can not be empty");
        
        Assert.hasText(config.getBucketName(), "bucketName is required");
        Assert.isTrue(config.getBucketName().matches("[A-Za-z0-9._-]+"), config.getBucketName() + " is not a valid bucket name");

        AwsS3CredentialsProviderChain.Builder providerChainBuilder = AwsS3CredentialsProviderChain.builder();

        if (config.getSessionToken() != null && config.getSessionToken().length() > 0) {
            providerChainBuilder.addCredentials(
                new BasicSessionCredentials(config.getAccessKey(), config.getSecretKey(), config.getSessionToken()));
        }
        else if (config.getAccessKey() != null && config.getAccessKey().length() > 0) {
            providerChainBuilder.addCredentials(
                new BasicSessionCredentials(config.getAccessKey(), config.getSecretKey(), config.getSessionToken()));
        }

        if (config.getProfile() != null && config.getProfile().length() > 0) {
            providerChainBuilder.addProfileCredentialsProvider(config.getProfile());
        }

        if (config.getUseDefaultCredentialsProvider()) {
            providerChainBuilder.addDefaultProviders();
        }

        ClientConfiguration cc = new ClientConfiguration();

        if (config.getProtocol() != null)
            cc.setProtocol(Protocol.valueOf(config.getProtocol().toString().toUpperCase()));

        if (config.isLegacySignatures()) cc.setSignerOverride("S3SignerType");

        if (config.getSocketTimeoutMs() >= 0) cc.setSocketTimeout(config.getSocketTimeoutMs());

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

        boolean bucketExists = s3.doesBucketExistV2(config.getBucketName());

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

            if (source != null) sourceReadWindow = source.getReadWindow();
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
                        time((Function<Void>) () -> {
                            s3.deleteObjects(new DeleteObjectsRequest(config.getBucketName()).withKeys(deleteVersions));
                            return null;
                        }, OPERATION_DELETE_OBJECTS);
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
                time((Function<Void>) () -> {
                    s3.deleteObject(config.getBucketName(), identifier);
                    return null;
                }, OPERATION_DELETE_OBJECT);
            } else {
                putObject(object, identifier);

                // if object has new metadata after the stream (i.e. encryption checksum), we must update S3 again
                if (object.isPostStreamUpdateRequired()) {
                    log.debug("[{}]: updating metadata after sync as required", object.getRelativePath());
                    final CopyObjectRequest cReq = new CopyObjectRequest(config.getBucketName(), identifier, config.getBucketName(), identifier);
                    cReq.setNewObjectMetadata(s3MetaFromSyncMeta(object.getMetadata()));
                    time((Function<Void>) () -> {
                        s3.copyObject(cReq);
                        return null;
                    }, OPERATION_UPDATE_METADATA);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to store object: " + e, e);
        }
    }

    @Override
    void putObject(SyncObject obj, String targetKey) {
        ObjectMetadata om;
        if (options.isSyncMetadata()) om = s3MetaFromSyncMeta(obj.getMetadata());
        else om = new ObjectMetadata();

        if (obj.getMetadata().isDirectory()) om.setContentType(TYPE_DIRECTORY);

        PutObjectRequest req;
        File file = (File) obj.getProperty(AbstractFilesystemStorage.PROP_FILE);
        S3ProgressListener progressListener = null;
        if (obj.getMetadata().isDirectory()) {
            req = new PutObjectRequest(config.getBucketName(), targetKey, new ByteArrayInputStream(new byte[0]), om);
        } else if (file != null) {
            req = new PutObjectRequest(config.getBucketName(), targetKey, file).withMetadata(om);
            progressListener = new ByteTransferListener(obj);
        } else {
            InputStream stream = obj.getDataStream();
            if (options.isMonitorPerformance())
                stream = new ProgressInputStream(stream, new PerformanceListener(getWriteWindow()));
            req = new PutObjectRequest(config.getBucketName(), targetKey, stream, om);
        }

        if (options.isSyncAcl())
            req.setAccessControlList(s3AclFromSyncAcl(obj.getAcl(), options.isIgnoreInvalidAcls()));

        TransferManager xferManager = null;
        try {
            // xfer manager will figure out if MPU is needed (based on threshold), do the MPU if necessary,
            // and abort if it fails
            xferManager = TransferManagerBuilder.standard()
                    .withS3Client(s3)
                    .withExecutorFactory(() -> Executors.newFixedThreadPool(config.getMpuThreadCount()))
                    .withMultipartUploadThreshold((long) config.getMpuThresholdMb() * 1024 * 1024)
                    .withMinimumUploadPartSize((long) config.getMpuPartSizeMb() * 1024 * 1024)
                    .withShutDownThreadPools(true)
                    .build();

            // directly update

            final Upload upload = xferManager.upload(req, progressListener);
            try {
                String eTag = time((Callable<String>) () -> upload.waitForUploadResult().getETag(), OPERATION_MPU);
                log.debug("Wrote {}, etag: {}", targetKey, eTag);
            } catch (Exception e) {
                log.error("upload exception", e);
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException("upload thread was interrupted", e);
            }
        } finally {
            // NOTE: apparently if we do not reference xferManager again after the upload() call (as in this finally
            // block), the JVM will for some crazy reason determine it is eligible for GC and call finalize(), which
            // shuts down the thread pool, fails the upload, and gives absolutely no indication of what's going on...
            if (xferManager != null) xferManager.shutdownNow(false);
        }
    }

    @Override
    public void delete(final String identifier) {
        time((Function<Void>) () -> {
            s3.deleteObject(config.getBucketName(), identifier);
            return null;
        }, OPERATION_DELETE_OBJECT);
    }

    // COMMON S3 CALLS

    private ObjectMetadata getS3Metadata(final String key, final String versionId) {
        return time(() -> {
            GetObjectMetadataRequest request = new GetObjectMetadataRequest(config.getBucketName(), key, versionId);
            return s3.getObjectMetadata(request);
        }, OPERATION_HEAD_OBJECT);
    }

    private AccessControlList getS3Acl(final String key, final String versionId) {
        return time(() -> {
            if (versionId == null) return s3.getObjectAcl(config.getBucketName(), key);
            else return s3.getObjectAcl(config.getBucketName(), key, versionId);
        }, OPERATION_GET_ACL);
    }

    private InputStream getS3DataStream(final String key, final String versionId) {
        return time((Function<InputStream>) () -> {
            GetObjectRequest request = new GetObjectRequest(config.getBucketName(), key, versionId);
            return s3.getObject(request).getObjectContent();
        }, OPERATION_OPEN_DATA_STREAM);
    }

    private List<S3VersionSummary> getS3Versions(final String key) {
        List<S3VersionSummary> versions = new ArrayList<>();

        VersionListing listing = null;
        do {
            final VersionListing fListing = listing;
            listing = time(() -> {
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
            }, OPERATION_LIST_VERSIONS);
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
        if (s3meta.getContentMD5() != null) meta.setChecksum(new Checksum("MD5", s3meta.getContentMD5()));
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
            om.setContentMD5(syncMeta.getChecksum().getValue());
        if (syncMeta.getContentType() != null) om.setContentType(syncMeta.getContentType());
        if (syncMeta.getHttpExpires() != null) om.setHttpExpiresDate(syncMeta.getHttpExpires());
        om.setUserMetadata(formatUserMetadata(syncMeta));
        if (syncMeta.getModificationTime() != null) om.setLastModified(syncMeta.getModificationTime());
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
        private String prefix;
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
        private String prefix;
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

    private class ByteTransferListener implements S3ProgressListener {
        private final SyncObject object;

        ByteTransferListener(SyncObject object) {
            this.object = object;
        }

        @Override
        public void onPersistableTransfer(PersistableTransfer persistableTransfer) {
        }

        @Override
        public void progressChanged(com.amazonaws.event.ProgressEvent progressEvent) {
            if (progressEvent.getEventType() == ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT) {
                if (sourceReadWindow != null) sourceReadWindow.increment(progressEvent.getBytesTransferred());
                if (options.isMonitorPerformance()) getWriteWindow().increment(progressEvent.getBytesTransferred());
                synchronized (object) {
                    // these events will include XML payload for MPU (no way to differentiate)
                    // do not set bytesRead to more then the object size
                    object.setBytesRead(object.getBytesRead() + progressEvent.getBytesTransferred());
                    if (object.getBytesRead() > object.getMetadata().getContentLength())
                        object.setBytesRead(object.getMetadata().getContentLength());
                }
            }
        }
    }
}

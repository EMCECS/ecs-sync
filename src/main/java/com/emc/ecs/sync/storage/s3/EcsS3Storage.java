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

import com.emc.ecs.sync.SyncTask;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.storage.ObjectNotFoundException;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.file.AbstractFilesystemStorage;
import com.emc.ecs.sync.util.*;
import com.emc.object.Protocol;
import com.emc.object.s3.*;
import com.emc.object.s3.bean.*;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.*;
import com.emc.object.util.ProgressInputStream;
import com.emc.object.util.ProgressListener;
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
import java.util.regex.Matcher;

import static com.emc.ecs.sync.config.storage.EcsS3Config.MIN_PART_SIZE_MB;

public class EcsS3Storage extends AbstractS3Storage<EcsS3Config> {
    private static final Logger log = LoggerFactory.getLogger(EcsS3Storage.class);

    // timed operations
    private static final String OPERATION_LIST_OBJECTS = "EcsS3ListObjects";
    private static final String OPERATION_LIST_VERSIONS = "EcsS3ListVersions";
    private static final String OPERATION_HEAD_OBJECT = "EcsS3HeadObject";
    private static final String OPERATION_GET_ACL = "EcsS3GetAcl";
    private static final String OPERATION_OPEN_DATA_STREAM = "EcsS3OpenDataStream";
    private static final String OPERATION_PUT_OBJECT = "EcsS3PutObject";
    private static final String OPERATION_MPU = "AwsS3MultipartUpload";
    private static final String OPERATION_DELETE_OBJECTS = "EcsS3DeleteObjects";
    private static final String OPERATION_DELETE_OBJECT = "EcsS3DeleteObject";
    private static final String OPERATION_UPDATE_METADATA = "EcsS3UpdateMetadata";
    private static final String OPERATION_REMOTE_COPY = "EcsS3RemoteCopy";

    private S3Client s3;
    private PerformanceWindow sourceReadWindow;
    private EcsS3Storage source;

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
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
                String portStr = config.getPort() > 0 ? "" + config.getPort() : "";
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
        s3Config.withIdentity(config.getAccessKey()).withSecretKey(config.getSecretKey());
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

        boolean bucketExists = s3.bucketExists(config.getBucketName());

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

        // if remote copy, make sure source is also S3
        if (config.isRemoteCopy()) {
            if (source instanceof EcsS3Storage) this.source = (EcsS3Storage) source;
            else throw new ConfigurationException("Remote copy is only supported between two ECS-S3 plugins");
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

    @Override
    @SuppressWarnings("unchecked")
    public void updateObject(final String identifier, SyncObject object) {
        try {
            // skip the root of the bucket since it obviously exists
            if ("".equals(identifier)) {
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
                    // can't modify objects during a remote copy
                    if (config.isRemoteCopy())
                        throw new RuntimeException("You cannot apply a transforming filter on a remote-copy");

                    log.debug("[{}]: updating metadata after sync as required", object.getRelativePath());
                    final CopyObjectRequest cReq = new CopyObjectRequest(config.getBucketName(), identifier, config.getBucketName(), identifier);
                    cReq.setObjectMetadata(s3MetaFromSyncMeta(object.getMetadata()));
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
    void putObject(SyncObject obj, final String targetKey) {
        S3ObjectMetadata om;
        if (options.isSyncMetadata()) om = s3MetaFromSyncMeta(obj.getMetadata());
        else om = new S3ObjectMetadata();

        if (obj.getMetadata().isDirectory()) om.setContentType(TYPE_DIRECTORY);
        AccessControlList acl = options.isSyncAcl() ? s3AclFromSyncAcl(obj.getAcl(), options.isIgnoreInvalidAcls()) : null;

        // differentiate single PUT or multipart upload
        long thresholdSize = (long) config.getMpuThresholdMb() * 1024 * 1024; // convert from MB
        if (config.isRemoteCopy()) {
            String sourceKey = source.getIdentifier(obj.getRelativePath(), obj.getMetadata().isDirectory());
            final CopyObjectRequest copyRequest = new CopyObjectRequest(source.getConfig().getBucketName(), sourceKey, config.getBucketName(), targetKey);
            if (obj instanceof S3ObjectVersion) copyRequest.setSourceVersionId(((S3ObjectVersion) obj).getVersionId());
            if (options.isSyncMetadata()) copyRequest.setObjectMetadata(om);
            else if (new Integer(0).equals(obj.getProperty(SyncTask.PROP_FAILURE_COUNT)))
                copyRequest.setIfTargetNoneMatch("*"); // special case for pure remote-copy (except on retries)
            if (options.isSyncAcl()) copyRequest.setAcl(acl);

            try {
                time((Function<Void>) () -> {
                    s3.copyObject(copyRequest);
                    return null;
                }, OPERATION_REMOTE_COPY);
            } catch (S3Exception e) {
                // special case for pure remote-copy; on 412, object already exists in target
                if (e.getHttpCode() != 412) throw e;
            }
        } else if (!config.isMpuEnabled() || obj.getMetadata().getContentLength() < thresholdSize) {
            Object data;
            if (obj.getMetadata().isDirectory()) {
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

            PutObjectResult result = time(() -> s3.putObject(req), OPERATION_PUT_OBJECT);

            log.debug("Wrote {} etag: {}", targetKey, result.getETag());
        } else {
            LargeFileUploader uploader;

            // we can read file parts in parallel
            File file = (File) obj.getProperty(AbstractFilesystemStorage.PROP_FILE);
            if (file != null) {
                uploader = new LargeFileUploader(s3, config.getBucketName(), targetKey, file);
                uploader.setProgressListener(new ByteTransferListener(obj));
            } else {
                uploader = new LargeFileUploader(s3, config.getBucketName(), targetKey, obj.getDataStream(), obj.getMetadata().getContentLength());
            }
            uploader.withPartSize((long) config.getMpuPartSizeMb() * 1024 * 1024).withThreads(config.getMpuThreadCount());
            uploader.setObjectMetadata(om);

            if (options.isSyncAcl()) uploader.setAcl(acl);

            final LargeFileUploader fUploader = uploader;
            time((Function<Void>) () -> {
                fUploader.doMultipartUpload();
                return null;
            }, OPERATION_MPU);
            log.debug("Wrote {} as MPU; etag: {}", targetKey, uploader.getETag());
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

    private S3ObjectMetadata getS3Metadata(final String key, final String versionId) {
        return time(() -> s3.getObjectMetadata(new GetObjectMetadataRequest(config.getBucketName(), key).withVersionId(versionId)), OPERATION_HEAD_OBJECT);
    }

    private AccessControlList getS3Acl(final String key, final String versionId) {
        return time(() -> s3.getObjectAcl(new GetObjectAclRequest(config.getBucketName(), key).withVersionId(versionId)), OPERATION_GET_ACL);
    }

    private InputStream getS3DataStream(final String key, final String versionId) {
        return time(() -> {
            GetObjectRequest request = new GetObjectRequest(config.getBucketName(), key).withVersionId(versionId);
            return s3.getObject(request, InputStream.class).getObject();
        }, OPERATION_OPEN_DATA_STREAM);
    }

    private List<AbstractVersion> getS3Versions(final String key) {
        List<AbstractVersion> versions = new ArrayList<>();

        ListVersionsResult listing = null;
        do {
            final ListVersionsResult fListing = listing;
            listing = time(() -> {
                if (fListing == null) {
                    return s3.listVersions(new ListVersionsRequest(config.getBucketName()).withPrefix(key).withDelimiter("/"));
                } else {
                    return s3.listMoreVersions(fListing);
                }
            }, OPERATION_LIST_VERSIONS);
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
        if (s3meta.getContentMd5() != null) meta.setChecksum(new Checksum("MD5", s3meta.getContentMd5()));
        meta.setContentType(s3meta.getContentType());
        meta.setHttpExpires(s3meta.getHttpExpires());
        meta.setExpirationDate(s3meta.getExpirationDate());
        meta.setModificationTime(s3meta.getLastModified());
        meta.setContentLength(s3meta.getContentLength());
        meta.setUserMetadata(toMetaMap(s3meta.getUserMetadata()));

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
            om.setContentMd5(syncMeta.getChecksum().getValue());
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
        return om;
    }

    private class PrefixIterator extends ReadOnlyIterator<ObjectSummary> {
        private String prefix;
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
        private String prefix;
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

    private class ByteTransferListener implements ProgressListener {
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

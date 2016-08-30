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
package com.emc.ecs.sync.target;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.object.FileSyncObject;
import com.emc.ecs.sync.model.object.S3ObjectVersion;
import com.emc.ecs.sync.model.object.S3SyncObject;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.S3Source;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.util.AwsS3Util;
import com.emc.ecs.sync.util.ConfigurationException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.concurrent.Executors;

public class S3Target extends SyncTarget {
    private static final Logger log = LoggerFactory.getLogger(S3Target.class);

    public static final String BUCKET_OPTION = "target-bucket";
    public static final String BUCKET_DESC = "Required. Specifies the target bucket to use";
    public static final String BUCKET_ARG_NAME = "bucket";

    public static final String CREATE_BUCKET_OPTION = "target-create-bucket";
    public static final String CREATE_BUCKET_DESC = "By default, the target bucket must exist. This option will create it if it does not";

    public static final String DISABLE_VHOSTS_OPTION = "target-disable-vhost";
    public static final String DISABLE_VHOSTS_DESC = "If specified, virtual hosted buckets will be disabled and path-style buckets will be used.";

    public static final String INCLUDE_VERSIONS_OPTION = "s3-include-versions";
    public static final String INCLUDE_VERSIONS_DESC = "Transfer all versions of every object. NOTE: this will overwrite all versions of each source key in the target system if any exist!";

    public static final String LEGACY_SIGNATURES_OPTION = "target-legacy-signatures";
    public static final String LEGACY_SIGNATURES_DESC = "If specified, the client will use v2 auth. Necessary for ECS.";

    public static final int DEFAULT_MPU_THRESHOLD_MB = 512;
    public static final String MPU_THRESHOLD_OPTION = "mpu-threshold";
    public static final String MPU_THRESHOLD_DESC = "Sets the size threshold (in MB) when an upload shall become a multipart upload.";
    public static final String MPU_THRESHOLD_ARG_NAME = "size-in-MB";

    public static final int DEFAULT_MPU_PART_SIZE_MB = 128;
    public static final String MPU_PART_SIZE_OPTION = "mpu-part-size";
    public static final String MPU_PART_SIZE_DESC = "Sets the part size to use when multipart upload is required (objects over 5GB). Default is " + DEFAULT_MPU_PART_SIZE_MB + "MB, minimum is " + AwsS3Util.MIN_PART_SIZE_MB + "MB.";
    public static final String MPU_PART_SIZE_ARG_NAME = "size-in-MB";

    public static final int DEFAULT_MPU_THREAD_COUNT = 4;
    public static final String MPU_THREAD_COUNT_OPTION = "mpu-thread-count";
    public static final String MPU_THREAD_COUNT_DESC = "The number of threads to use for multipart upload (only applicable for file sources).";
    public static final String MPU_THREAD_COUNT_ARG_NAME = "thread-count";

    public static final String SOCKET_TIMEOUT_OPTION = "target-socket-timeout";
    public static final String SOCKET_TIMEOUT_DESC = "Sets the socket timeout in milliseconds (default is " + ClientConfiguration.DEFAULT_SOCKET_TIMEOUT + "ms)";
    public static final String SOCKET_TIMEOUT_ARG_NAME = "timeout-ms";

    public static final String NO_PRESERVE_DIRS_OPTION = "no-preserve-dirs";
    public static final String NO_PRESERVE_DIRS_DESC = "Directories will not be preserved. By default, directories are stored as empty objects to preserve empty dirs and metadata from the source.";

    private String protocol;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String bucketName;
    private String rootKey;
    private boolean disableVHosts;
    private boolean createBucket;
    private boolean includeVersions;
    private boolean legacySignatures;
    private int mpuThresholdMB = DEFAULT_MPU_THRESHOLD_MB;
    private int mpuPartSizeMB = DEFAULT_MPU_PART_SIZE_MB;
    private int mpuThreadCount = DEFAULT_MPU_THREAD_COUNT;
    private int socketTimeoutMs = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;
    private boolean preserveDirectories = true;
    private S3Source s3Source;

    private AmazonS3 s3;

    @Override
    public boolean canHandleTarget(String targetUri) {
        return targetUri.startsWith(AwsS3Util.URI_PREFIX);
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt(BUCKET_OPTION).desc(BUCKET_DESC)
                .hasArg().argName(BUCKET_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(DISABLE_VHOSTS_OPTION).desc(DISABLE_VHOSTS_DESC).build());
        opts.addOption(Option.builder().longOpt(CREATE_BUCKET_OPTION).desc(CREATE_BUCKET_DESC).build());
        opts.addOption(Option.builder().longOpt(INCLUDE_VERSIONS_OPTION).desc(INCLUDE_VERSIONS_DESC).build());
        opts.addOption(Option.builder().longOpt(LEGACY_SIGNATURES_OPTION).desc(LEGACY_SIGNATURES_DESC).build());
        opts.addOption(Option.builder().longOpt(MPU_THRESHOLD_OPTION).desc(MPU_THRESHOLD_DESC)
                .hasArg().argName(MPU_THRESHOLD_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(MPU_PART_SIZE_OPTION).desc(MPU_PART_SIZE_DESC)
                .hasArg().argName(MPU_PART_SIZE_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(MPU_THREAD_COUNT_OPTION).desc(MPU_THREAD_COUNT_DESC)
                .hasArg().argName(MPU_THREAD_COUNT_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(SOCKET_TIMEOUT_OPTION).desc(SOCKET_TIMEOUT_DESC)
                .hasArg().argName(SOCKET_TIMEOUT_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(NO_PRESERVE_DIRS_OPTION).desc(NO_PRESERVE_DIRS_DESC).build());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        AwsS3Util.S3Uri s3Uri = AwsS3Util.parseUri(targetUri);
        protocol = s3Uri.protocol;
        endpoint = s3Uri.endpoint;
        accessKey = s3Uri.accessKey;
        secretKey = s3Uri.secretKey;
        rootKey = s3Uri.rootKey;

        if (line.hasOption(BUCKET_OPTION)) bucketName = line.getOptionValue(BUCKET_OPTION);

        disableVHosts = line.hasOption(DISABLE_VHOSTS_OPTION);

        createBucket = line.hasOption(CREATE_BUCKET_OPTION);

        includeVersions = line.hasOption(INCLUDE_VERSIONS_OPTION);

        legacySignatures = line.hasOption(LEGACY_SIGNATURES_OPTION);

        if (line.hasOption(MPU_THRESHOLD_OPTION))
            mpuThresholdMB = Integer.parseInt(line.getOptionValue(MPU_THRESHOLD_OPTION));
        if (line.hasOption(MPU_PART_SIZE_OPTION))
            mpuPartSizeMB = Integer.parseInt(line.getOptionValue(MPU_PART_SIZE_OPTION));
        if (line.hasOption(MPU_THREAD_COUNT_OPTION))
            mpuThreadCount = Integer.parseInt(line.getOptionValue(MPU_THREAD_COUNT_OPTION));

        if (line.hasOption(SOCKET_TIMEOUT_OPTION))
            socketTimeoutMs = Integer.parseInt(line.getOptionValue(SOCKET_TIMEOUT_OPTION));

        if (line.hasOption(NO_PRESERVE_DIRS_OPTION))
            preserveDirectories = false;
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        Assert.hasText(accessKey, "accessKey is required");
        Assert.hasText(secretKey, "secretKey is required");
        Assert.hasText(bucketName, "bucketName is required");
        Assert.isTrue(bucketName.matches("[A-Za-z0-9._-]+"), bucketName + " is not a valid bucket name");

        AWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration config = new ClientConfiguration();

        if (protocol != null) config.setProtocol(Protocol.valueOf(protocol.toUpperCase()));

        if (legacySignatures) config.setSignerOverride("S3SignerType");

        if (socketTimeoutMs >= 0) config.setSocketTimeout(socketTimeoutMs);

        s3 = new AmazonS3Client(creds, config);

        if (endpoint != null) s3.setEndpoint(endpoint);

        // TODO: generalize uri translation
        AwsS3Util.S3Uri s3Uri = new AwsS3Util.S3Uri();
        s3Uri.protocol = protocol;
        s3Uri.endpoint = endpoint;
        s3Uri.accessKey = accessKey;
        s3Uri.secretKey = secretKey;
        s3Uri.rootKey = rootKey;
        if (targetUri == null) targetUri = s3Uri.toUri();

        if (disableVHosts) {
            log.info("The use of virtual hosted buckets on the s3 source has been DISABLED.  Path style buckets will be used.");
            S3ClientOptions opts = new S3ClientOptions();
            opts.setPathStyleAccess(true);
            s3.setS3ClientOptions(opts);
        }

        // for version support. TODO: genericize version support
        if (source instanceof S3Source) {
            s3Source = (S3Source) source;
            if (!s3Source.isVersioningEnabled()) includeVersions = false; // don't include versions if source versioning is off
        } else if (includeVersions) {
            throw new ConfigurationException("Object versions are currently only supported with the S3 source & target plugins.");
        }

        if (!s3.doesBucketExist(bucketName)) {
            if (createBucket) {
                s3.createBucket(bucketName);
                if (includeVersions)
                    s3.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucketName,
                            new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
            } else {
                throw new ConfigurationException("The bucket " + bucketName + " does not exist.");
            }
        }

        if (rootKey == null) rootKey = ""; // make sure rootKey isn't null

        if (includeVersions) {
            String status = s3.getBucketVersioningConfiguration(bucketName).getStatus();
            if (BucketVersioningConfiguration.OFF.equals(status))
                throw new ConfigurationException("The specified bucket does not have versioning enabled.");
        }

        if (mpuThresholdMB > AwsS3Util.MAX_PUT_SIZE_MB) {
            log.warn("{}MB is above the maximum PUT size of {}MB. the maximum will be used instead",
                    mpuThresholdMB, AwsS3Util.MAX_PUT_SIZE_MB);
            mpuThresholdMB = AwsS3Util.MAX_PUT_SIZE_MB;
        }
        if (mpuPartSizeMB < AwsS3Util.MIN_PART_SIZE_MB) {
            log.warn("{}MB is below the minimum MPU part size of {}MB. the minimum will be used instead",
                    mpuPartSizeMB, AwsS3Util.MIN_PART_SIZE_MB);
            mpuPartSizeMB = AwsS3Util.MIN_PART_SIZE_MB;
        }
    }

    @Override
    public void filter(SyncObject obj) {
        try {
            // skip the root of the bucket since it obviously exists
            if ("".equals(rootKey + obj.getRelativePath())) {
                log.debug("Target is bucket root; skipping");
                return;
            }

            // check early on to see if we should ignore directories
            if (!preserveDirectories && obj.isDirectory()) {
                log.debug("Source is directory and preserveDirectories is false; skipping");
                return;
            }

            // some sync objects lazy-load their metadata (i.e. AtmosSyncObject)
            // since this may be a timed operation, ensure it loads outside of other timed operations
            if (!(obj instanceof S3ObjectVersion) || !((S3ObjectVersion) obj).isDeleteMarker())
                obj.getMetadata();

            // Compute target key
            String targetKey = getTargetKey(obj);
            obj.setTargetIdentifier(AwsS3Util.fullPath(bucketName, targetKey));

            if (includeVersions) {
                ListIterator<S3ObjectVersion> sourceVersions = s3Source.versionIterator((S3SyncObject) obj);
                ListIterator<S3ObjectVersion> targetVersions = versionIterator(obj);

                boolean newVersions = false, replaceVersions = false;
                if (force) {
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
                                putIntermediateVersions(sourceVersions, targetKey); // add any new intermediary versions (current is added below)
                            }
                        }

                        if (targetVersions.hasNext()) replaceVersions = true; // target has more versions

                        if (!newVersions && !replaceVersions) {
                            log.info("Source and target versions are the same. Skipping {}", obj.getRelativePath());
                            return;
                        }
                    }
                }

                // something's off; must delete all versions of the object
                if (replaceVersions) {
                    log.info("[{}]: version history differs between source and target; re-placing target version history with that from source.",
                            obj.getRelativePath());

                    // collect versions in target
                    List<DeleteObjectsRequest.KeyVersion> deleteVersions = new ArrayList<>();
                    while (targetVersions.hasNext()) targetVersions.next(); // move cursor to end
                    while (targetVersions.hasPrevious()) { // go in reverse order
                        S3ObjectVersion version = targetVersions.previous();
                        deleteVersions.add(new DeleteObjectsRequest.KeyVersion(targetKey, version.getVersionId()));
                    }

                    // batch delete all versions in target
                    log.debug("[{}]: deleting all versions in target", obj.getRelativePath());
                    s3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(deleteVersions));

                    // replay version history in target
                    while (sourceVersions.hasPrevious()) sourceVersions.previous(); // move cursor to beginning
                    putIntermediateVersions(sourceVersions, targetKey);
                }

            } else { // normal sync (no versions)
                Date sourceLastModified = obj.getMetadata().getModificationTime();
                long sourceSize = obj.getMetadata().getContentLength();

                // Get target metadata.
                ObjectMetadata destMeta = null;
                try {
                    destMeta = s3.getObjectMetadata(bucketName, targetKey);
                } catch (AmazonS3Exception e) {
                    if (e.getStatusCode() != 404)
                        throw new RuntimeException("Failed to check target key '" + targetKey + "' : " + e, e);
                }

                if (!force && obj.getFailureCount() == 0 && destMeta != null) {

                    // Check overwrite
                    Date destLastModified = destMeta.getLastModified();
                    long destSize = destMeta.getContentLength();

                    if (destLastModified.equals(sourceLastModified) && sourceSize == destSize) {
                        log.info("Source and target the same.  Skipping {}", obj.getRelativePath());
                        return;
                    }
                    if (destLastModified.after(sourceLastModified)) {
                        log.info("Target newer than source.  Skipping {}", obj.getRelativePath());
                        return;
                    }
                }
            }

            // at this point we know we are going to write the object
            // Put [current object version]
            if (obj instanceof S3ObjectVersion && ((S3ObjectVersion) obj).isDeleteMarker()) {

                // object has version history, but is currently deleted
                log.debug("[{}]: deleting object in target to replicate delete marker in source.", obj.getRelativePath());
                s3.deleteObject(bucketName, targetKey);
            } else {
                putObject(obj, targetKey);

                // if object has new metadata after the stream (i.e. encryption checksum), we must update S3 again
                if (obj.requiresPostStreamMetadataUpdate()) {
                    log.debug("[{}]: updating metadata after sync as required", obj.getRelativePath());
                    CopyObjectRequest cReq = new CopyObjectRequest(bucketName, targetKey, bucketName, targetKey);
                    cReq.setNewObjectMetadata(AwsS3Util.s3MetaFromSyncMeta(obj.getMetadata()));
                    s3.copyObject(cReq);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to store object: " + e, e);
        }
    }

    protected void putIntermediateVersions(ListIterator<S3ObjectVersion> versions, String key) {
        while (versions.hasNext()) {
            S3ObjectVersion version = versions.next();
            try {
                if (!version.isLatest()) {
                    // source has more versions; add any non-current versions that are missing from the target
                    // (current version will be added below)
                    if (version.isDeleteMarker()) {
                        log.debug("[{}#{}]: deleting object in target to replicate delete marker in source.",
                                version.getRelativePath(), version.getVersionId());
                        s3.deleteObject(bucketName, key);
                    } else {
                        log.debug("[{}#{}]: replicating historical version in target.",
                                version.getRelativePath(), version.getVersionId());
                        putObject(version, key);
                    }
                }
            } catch (RuntimeException e) {
                throw new RuntimeException(String.format("sync of historical version %s failed", version.getVersionId()), e);
            }
        }
    }

    protected void putObject(SyncObject obj, String targetKey) {
        ObjectMetadata om = AwsS3Util.s3MetaFromSyncMeta(obj.getMetadata());
        if (obj.isDirectory()) om.setContentType(AwsS3Util.TYPE_DIRECTORY);

        PutObjectRequest req;
        if (obj.isDirectory()) {
            req = new PutObjectRequest(bucketName, targetKey, new ByteArrayInputStream(new byte[0]), om);
        } else if (obj instanceof FileSyncObject) {
            req = new PutObjectRequest(bucketName, targetKey, ((FileSyncObject) obj).getRawSourceIdentifier()).withMetadata(om);
        } else {
            req = new PutObjectRequest(bucketName, targetKey, obj.getInputStream(), om);
        }

        if (includeAcl)
            req.setAccessControlList(AwsS3Util.s3AclFromSyncAcl(obj.getMetadata().getAcl(), ignoreInvalidAcls));

        // xfer manager will figure out if MPU is needed (based on threshold), do the MPU if necessary,
        // and abort if it fails
        TransferManagerConfiguration xferConfig = new TransferManagerConfiguration();
        xferConfig.setMultipartUploadThreshold((long) mpuThresholdMB * 1024 * 1024);
        xferConfig.setMinimumUploadPartSize((long) mpuPartSizeMB * 1024 * 1024);
        TransferManager xferManager = new TransferManager(s3, Executors.newFixedThreadPool(mpuThreadCount));
        xferManager.setConfiguration(xferConfig);

        Upload upload = xferManager.upload(req);
        try {
            log.debug("Wrote {}, etag: {}", targetKey, upload.waitForUploadResult().getETag());
        } catch (InterruptedException e) {
            throw new RuntimeException("upload thread was interrupted", e);
        } finally {
            // make sure bytes read is accurate if we bypassed the counting stream
            if (obj instanceof FileSyncObject) {
                try {
                    ((FileSyncObject) obj).setOverrideBytesRead(upload.getProgress().getBytesTransferred());
                } catch (Throwable t) {
                    log.warn("could not get bytes transferred from upload", t);
                }
            }
        }
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        obj.setTargetIdentifier(getTargetKey(obj));
        return new S3SyncObject(this, s3, bucketName, getTargetKey(obj), obj.getRelativePath());
    }

    public ListIterator<S3ObjectVersion> versionIterator(SyncObject obj) {
        return AwsS3Util.listVersions(this, s3, bucketName, getTargetKey(obj), obj.getRelativePath());
    }

    private String getTargetKey(SyncObject obj) {
        String targetKey = rootKey + obj.getRelativePath();
        if (obj.isDirectory() && !targetKey.endsWith("/")) targetKey += "/"; // dir placeholders must end with slash
        return targetKey;
    }

    @Override
    public String getName() {
        return "S3 Target";
    }

    @Override
    public String getDocumentation() {
        return "Target that writes content to an S3 bucket.  This " +
                "target plugin is triggered by the pattern:\n" +
                AwsS3Util.PATTERN_DESC + "\n" +
                "Scheme, host and port are all optional. If omitted, " +
                "https://s3.amazonaws.com:443 is assumed. " +
                "root-prefix (optional) is the prefix to prepend to key names " +
                "when writing objects e.g. dir1/. If omitted, objects " +
                "will be written to the root of the bucket. Note that this plugin also " +
                "accepts the --force option to force overwriting target objects " +
                "even if they are the same or newer than the source.";
    }

    @Override
    public String summarizeConfig() {
        return super.summarizeConfig()
                + " - mpuThresholdMB: " + mpuThresholdMB + "\n"
                + " - mpuPartSizeMB: " + mpuPartSizeMB + "\n"
                + " - mpuThreadCount: " + mpuThreadCount + "\n";
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getRootKey() {
        return rootKey;
    }

    public void setRootKey(String rootKey) {
        this.rootKey = rootKey;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public boolean isDisableVHosts() {
        return disableVHosts;
    }

    public void setDisableVHosts(boolean disableVHosts) {
        this.disableVHosts = disableVHosts;
    }

    public boolean isCreateBucket() {
        return createBucket;
    }

    public void setCreateBucket(boolean createBucket) {
        this.createBucket = createBucket;
    }

    public boolean isIncludeVersions() {
        return includeVersions;
    }

    public void setIncludeVersions(boolean includeVersions) {
        this.includeVersions = includeVersions;
    }

    public boolean isLegacySignatures() {
        return legacySignatures;
    }

    public void setLegacySignatures(boolean legacySignatures) {
        this.legacySignatures = legacySignatures;
    }

    public int getMpuThresholdMB() {
        return mpuThresholdMB;
    }

    public void setMpuThresholdMB(int mpuThresholdMB) {
        this.mpuThresholdMB = mpuThresholdMB;
    }

    public int getMpuPartSizeMB() {
        return mpuPartSizeMB;
    }

    public void setMpuPartSizeMB(int mpuPartSizeMB) {
        this.mpuPartSizeMB = mpuPartSizeMB;
    }

    public int getMpuThreadCount() {
        return mpuThreadCount;
    }

    public void setMpuThreadCount(int mpuThreadCount) {
        this.mpuThreadCount = mpuThreadCount;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public void setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }

    public boolean isPreserveDirectories() {
        return preserveDirectories;
    }

    public void setPreserveDirectories(boolean preserveDirectories) {
        this.preserveDirectories = preserveDirectories;
    }
}

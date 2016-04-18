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

import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.object.*;
import com.emc.ecs.sync.source.EcsS3Source;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.util.AwsS3Util;
import com.emc.ecs.sync.util.ConfigurationException;
import com.emc.ecs.sync.util.EcsS3Util;
import com.emc.ecs.sync.util.Function;
import com.emc.object.s3.*;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.ObjectKey;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.bean.VersioningConfiguration;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.CopyObjectRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.util.ProgressInputStream;
import com.emc.object.util.ProgressListener;
import com.emc.rest.smart.ecs.Vdc;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.InputStream;
import java.net.URI;
import java.util.*;

public class EcsS3Target extends SyncTarget {
    private static final Logger log = LoggerFactory.getLogger(EcsS3Target.class);

    public static final String BUCKET_OPTION = "target-bucket";
    public static final String BUCKET_DESC = "Required. Specifies the target bucket to use";
    public static final String BUCKET_ARG_NAME = "bucket";

    public static final String CREATE_BUCKET_OPTION = "target-create-bucket";
    public static final String CREATE_BUCKET_DESC = "By default, the target bucket must exist. This option will create it if it does not";

    public static final String ENABLE_VHOSTS_OPTION = "target-enable-vhost";
    public static final String ENABLE_VHOSTS_DESC = "If specified, virtual hosted buckets will be disabled and path-style buckets will be used.";

    public static final String NO_SMART_CLIENT_OPTION = "target-no-smart-client";
    public static final String NO_SMART_CLIENT_DESC = "Disables the smart client (client-side load balancing). Necessary when using a proxy or external load balancer without DNS configuration.";

    public static final String INCLUDE_VERSIONS_OPTION = "s3-include-versions";
    public static final String INCLUDE_VERSIONS_DESC = "Transfer all versions of every object. NOTE: this will overwrite all versions of each source key in the target system if any exist!";

    public static final String APACHE_CLIENT_OPTION = "target-apache-client";
    public static final String APACHE_CLIENT_DESC = "If specified, target will use the Apache HTTP client, which is not as efficient, but enables Expect: 100-Continue (header pre-flight).";

    public static final int DEFAULT_MPU_THRESHOLD_MB = 512;
    public static final String MPU_THRESHOLD_OPTION = "mpu-threshold";
    public static final String MPU_THRESHOLD_DESC = "Sets the size threshold (in MB) when a PUT object shall become a multipart upload.";
    public static final String MPU_THRESHOLD_ARG_NAME = "size-in-MB";

    public static final int DEFAULT_MPU_PART_SIZE_MB = 128;
    public static final String MPU_PART_SIZE_OPTION = "mpu-part-size";
    public static final String MPU_PART_SIZE_DESC = "Sets the part size to use when multipart upload is required (objects over 5GB). Default is " + DEFAULT_MPU_PART_SIZE_MB + "MB, minimum is " + EcsS3Util.MIN_PART_SIZE_MB + "MB.";
    public static final String MPU_PART_SIZE_ARG_NAME = "size-in-MB";

    public static final int DEFAULT_MPU_THREAD_COUNT = 4;
    public static final String MPU_THREAD_COUNT_OPTION = "mpu-thread-count";
    public static final String MPU_THREAD_COUNT_DESC = "The number of threads to use for multipart upload (only applicable for file sources).";
    public static final String MPU_THREAD_COUNT_ARG_NAME = "thread-count";

    public static final String DISABLE_MPU_OPTION = "disable-mpu";
    public static final String DISABLE_MPU_DESC = "Disables multipart upload (all uploads will be a single PUT operation)";

    public static final String NO_PRESERVE_DIRS_OPTION = "no-preserve-dirs";
    public static final String NO_PRESERVE_DIRS_DESC = "Directories will not be preserved. By default, directories are stored as empty objects to preserve empty dirs and metadata from the source.";

    public static final String OPERATION_DELETE_VERSIONS = "EcsS3DeleteVersions";
    public static final String OPERATION_DELETE_OBJECT = "EcsS3DeleteObject";
    public static final String OPERATION_GET_METADATA = "EcsS3GetMetadata";
    public static final String OPERATION_UPDATE_METADATA = "EcsS3UpdateMetadata";
    public static final String OPERATION_PUT_OBJECT = "EcsS3PutObject";
    public static final String OPERATION_MPU = "EcsS3MultipartUpload";

    private String protocol;
    private List<Vdc> vdcs;
    private int port;
    private URI endpoint;
    private String accessKey;
    private String secretKey;
    private boolean enableVHosts;
    private boolean smartClientEnabled = true;
    private String bucketName;
    private String rootKey;
    private boolean createBucket;
    private boolean includeVersions;
    private boolean apacheClientEnabled;
    private int mpuThresholdMB = DEFAULT_MPU_THRESHOLD_MB;
    private int mpuPartSizeMB = DEFAULT_MPU_PART_SIZE_MB;
    private int mpuThreadCount = DEFAULT_MPU_THREAD_COUNT;
    private boolean mpuDisabled;
    private boolean preserveDirectories = true;
    private EcsS3Source s3Source;

    private S3Client s3;

    @Override
    public boolean canHandleTarget(String targetUri) {
        return targetUri.startsWith(EcsS3Util.URI_PREFIX);
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt(BUCKET_OPTION).desc(BUCKET_DESC)
                .hasArg().argName(BUCKET_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(CREATE_BUCKET_OPTION).desc(CREATE_BUCKET_DESC).build());
        opts.addOption(Option.builder().longOpt(ENABLE_VHOSTS_OPTION).desc(ENABLE_VHOSTS_DESC).build());
        opts.addOption(Option.builder().longOpt(NO_SMART_CLIENT_OPTION).desc(NO_SMART_CLIENT_DESC).build());
        opts.addOption(Option.builder().longOpt(INCLUDE_VERSIONS_OPTION).desc(INCLUDE_VERSIONS_DESC).build());
        opts.addOption(Option.builder().longOpt(APACHE_CLIENT_OPTION).desc(APACHE_CLIENT_DESC).build());
        opts.addOption(Option.builder().longOpt(MPU_THRESHOLD_OPTION).desc(MPU_THRESHOLD_DESC)
                .hasArg().argName(MPU_THRESHOLD_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(MPU_PART_SIZE_OPTION).desc(MPU_PART_SIZE_DESC)
                .hasArg().argName(MPU_PART_SIZE_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(MPU_THREAD_COUNT_OPTION).desc(MPU_THREAD_COUNT_DESC)
                .hasArg().argName(MPU_THREAD_COUNT_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(DISABLE_MPU_OPTION).desc(DISABLE_MPU_DESC).build());
        opts.addOption(Option.builder().longOpt(NO_PRESERVE_DIRS_OPTION).desc(NO_PRESERVE_DIRS_DESC).build());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        EcsS3Util.S3Uri s3Uri = EcsS3Util.parseUri(targetUri);
        protocol = s3Uri.protocol;
        vdcs = s3Uri.vdcs;
        port = s3Uri.port;
        accessKey = s3Uri.accessKey;
        secretKey = s3Uri.secretKey;
        rootKey = s3Uri.rootKey;
        endpoint = s3Uri.getEndpointUri();

        if (line.hasOption(BUCKET_OPTION))
            bucketName = line.getOptionValue(BUCKET_OPTION);

        createBucket = line.hasOption(CREATE_BUCKET_OPTION);

        enableVHosts = line.hasOption(ENABLE_VHOSTS_OPTION);

        smartClientEnabled = !line.hasOption(NO_SMART_CLIENT_OPTION);

        apacheClientEnabled = line.hasOption(APACHE_CLIENT_OPTION);

        includeVersions = line.hasOption(INCLUDE_VERSIONS_OPTION);

        if (line.hasOption(MPU_THRESHOLD_OPTION))
            mpuThresholdMB = Integer.parseInt(line.getOptionValue(MPU_THRESHOLD_OPTION));
        if (line.hasOption(MPU_PART_SIZE_OPTION))
            mpuPartSizeMB = Integer.parseInt(line.getOptionValue(MPU_PART_SIZE_OPTION));
        if (line.hasOption(MPU_THREAD_COUNT_OPTION))
            mpuThreadCount = Integer.parseInt(line.getOptionValue(MPU_THREAD_COUNT_OPTION));
        mpuDisabled = line.hasOption(DISABLE_MPU_OPTION);

        if (line.hasOption(NO_PRESERVE_DIRS_OPTION))
            preserveDirectories = false;
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        Assert.hasText(accessKey, "accessKey is required");
        Assert.hasText(secretKey, "secretKey is required");
        Assert.hasText(bucketName, "bucketName is required");
        Assert.isTrue(bucketName.matches("[A-Za-z0-9._-]+"), bucketName + " is not a valid bucket name");

        S3Config s3Config;
        if (enableVHosts) {
            Assert.notNull(endpoint, "endpoint is required");
            s3Config = new S3Config(endpoint);
        } else {
            // try to infer from endpoint
            if (endpoint != null) {
                if (vdcs == null && endpoint.getHost() != null) {
                    vdcs = new ArrayList<>();
                    for (String host : endpoint.getHost().split(",")) {
                        vdcs.add(new Vdc(host));
                    }
                }
                if (port <= 0 && endpoint.getPort() > 0) port = endpoint.getPort();
                if (protocol == null && endpoint.getScheme() != null) protocol = endpoint.getScheme();
            }
            Assert.hasText(protocol, "protocol is required");
            Assert.notEmpty(vdcs, "at least one VDC is required");
            s3Config = new S3Config(com.emc.object.Protocol.valueOf(protocol.toUpperCase()), vdcs.toArray(new Vdc[vdcs.size()]));
            if (port > 0) s3Config.setPort(port);
            s3Config.setSmartClient(smartClientEnabled);
        }
        s3Config.withIdentity(accessKey).withSecretKey(secretKey);

        if (apacheClientEnabled) {
            s3 = new S3JerseyClient(s3Config);
        } else {
            System.setProperty("http.maxConnections", "100");
            s3 = new S3JerseyClient(s3Config, new URLConnectionClientHandler());
        }

        // TODO: generalize uri translation
        EcsS3Util.S3Uri s3Uri = new EcsS3Util.S3Uri();
        s3Uri.protocol = protocol;
        s3Uri.vdcs = vdcs;
        s3Uri.port = port;
        s3Uri.accessKey = accessKey;
        s3Uri.secretKey = secretKey;
        s3Uri.rootKey = rootKey;
        if (targetUri == null) targetUri = s3Uri.toUri();

        // for version support. TODO: genericize version support
        if (source instanceof EcsS3Source) {
            s3Source = (EcsS3Source) source;
            if (!s3Source.isVersioningEnabled()) includeVersions = false; // don't include versions if source versioning is off
        } else if (includeVersions) {
            throw new ConfigurationException("Object versions are currently only supported with the S3 source & target plugins.");
        }

        if (!s3.bucketExists(bucketName)) {
            if (createBucket) {
                s3.createBucket(bucketName);
                if (includeVersions)
                    s3.setBucketVersioning(bucketName,
                            new VersioningConfiguration().withStatus(VersioningConfiguration.Status.Enabled));
            } else {
                throw new ConfigurationException("The bucket " + bucketName + " does not exist.");
            }
        }

        if (rootKey == null) rootKey = ""; // make sure rootKey isn't null

        if (includeVersions) {
            VersioningConfiguration.Status status = s3.getBucketVersioning(bucketName).getStatus();
            if (status == null || status == VersioningConfiguration.Status.Suspended)
                throw new ConfigurationException("The specified bucket does not have versioning enabled.");
        }

        if (!mpuDisabled && mpuPartSizeMB < EcsS3Util.MIN_PART_SIZE_MB) {
            log.warn("{}MB is below the minimum MPU part size of {}MB. the minimum will be used instead",
                    mpuPartSizeMB, EcsS3Util.MIN_PART_SIZE_MB);
            mpuPartSizeMB = EcsS3Util.MIN_PART_SIZE_MB;
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
            if (!(obj instanceof EcsS3ObjectVersion) || !((EcsS3ObjectVersion) obj).isDeleteMarker())
                obj.getMetadata();

            // Compute target key
            final String targetKey = getTargetKey(obj);
            obj.setTargetIdentifier(AwsS3Util.fullPath(bucketName, targetKey));

            if (includeVersions) {
                ListIterator<EcsS3ObjectVersion> sourceVersions = s3Source.versionIterator((EcsS3SyncObject) obj);
                ListIterator<EcsS3ObjectVersion> targetVersions = versionIterator(obj);

                boolean newVersions = false, replaceVersions = false;
                if (force) {
                    replaceVersions = true;
                } else {

                    // special workaround for bug where objects are listed, but they have no versions
                    if (sourceVersions.hasNext()) {

                        // check count and etag/delete-marker to compare version chain
                        while (sourceVersions.hasNext()) {
                            EcsS3ObjectVersion sourceVersion = sourceVersions.next();

                            if (targetVersions.hasNext()) {
                                EcsS3ObjectVersion targetVersion = targetVersions.next();

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
                            log.info("Source and target versions are the same.  Skipping {}", obj.getRelativePath());
                            return;
                        }
                    }
                }

                // something's off; must delete all versions of the object
                if (replaceVersions) {
                    log.info("[{}]: version history differs between source and target; re-placing target version history with that from source.",
                            obj.getRelativePath());

                    // collect versions in target
                    final List<ObjectKey> deleteVersions = new ArrayList<>();
                    while (targetVersions.hasNext()) targetVersions.next(); // move cursor to end
                    while (targetVersions.hasPrevious()) { // go in reverse order
                        EcsS3ObjectVersion version = targetVersions.previous();
                        deleteVersions.add(new ObjectKey(targetKey, version.getVersionId()));
                    }

                    // batch delete all versions in target
                    log.debug("[{}]: deleting all versions in target", obj.getRelativePath());
                    time(new Function<Void>() {
                        @Override
                        public Void call() {
                            s3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(deleteVersions));
                            return null;
                        }
                    }, OPERATION_DELETE_VERSIONS);

                    // replay version history in target
                    while (sourceVersions.hasPrevious()) sourceVersions.previous(); // move cursor to beginning
                    putIntermediateVersions(sourceVersions, targetKey);
                }

            } else { // normal sync (no versions)
                Date sourceLastModified = obj.getMetadata().getModificationTime();
                long sourceSize = obj.getMetadata().getContentLength();

                // Get target metadata.
                S3ObjectMetadata destMeta = null;
                try {
                    destMeta = time(new Function<S3ObjectMetadata>() {
                        @Override
                        public S3ObjectMetadata call() {
                            return s3.getObjectMetadata(bucketName, targetKey);
                        }
                    }, OPERATION_GET_METADATA);
                } catch (S3Exception e) {
                    if (e.getHttpCode() != 404) {
                        throw new RuntimeException("Failed to check target key '" + targetKey + "' : " + e, e);
                    }
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
                time(new Function<Void>() {
                    @Override
                    public Void call() {
                        s3.deleteObject(bucketName, targetKey);
                        return null;
                    }
                }, OPERATION_DELETE_OBJECT);
            } else {
                putObject(obj, targetKey);

                // if object has new metadata after the stream (i.e. encryption checksum), we must update S3 again
                if (obj.requiresPostStreamMetadataUpdate()) {
                    log.debug("[{}]: updating metadata after sync as required", obj.getRelativePath());
                    final CopyObjectRequest cReq = new CopyObjectRequest(bucketName, targetKey, bucketName, targetKey);
                    cReq.setObjectMetadata(EcsS3Util.s3MetaFromSyncMeta(obj.getMetadata()));
                    time(new Function<Void>() {
                        @Override
                        public Void call() {
                            s3.copyObject(cReq);
                            return null;
                        }
                    }, OPERATION_UPDATE_METADATA);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to store object: " + e, e);
        }
    }

    protected void putIntermediateVersions(ListIterator<EcsS3ObjectVersion> versions, final String key) {
        while (versions.hasNext()) {
            EcsS3ObjectVersion version = versions.next();
            try {
                if (!version.isLatest()) {
                    // source has more versions; add any non-current versions that are missing from the target
                    // (current version will be added below)
                    if (version.isDeleteMarker()) {
                        log.debug("[{}#{}]: deleting object in target to replicate delete marker in source.",
                                version.getRelativePath(), version.getVersionId());
                        time(new Function<Void>() {
                            @Override
                            public Void call() {
                                s3.deleteObject(bucketName, key);
                                return null;
                            }
                        }, OPERATION_DELETE_OBJECT);
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

    protected void putObject(SyncObject obj, final String targetKey) {
        S3ObjectMetadata om = EcsS3Util.s3MetaFromSyncMeta(obj.getMetadata());
        if (obj.isDirectory()) om.setContentType(AwsS3Util.TYPE_DIRECTORY);
        AccessControlList acl = includeAcl ? EcsS3Util.s3AclFromSyncAcl(obj.getMetadata().getAcl(), ignoreInvalidAcls) : null;

        // differentiate single PUT or multipart upload
        long thresholdSize = (long) mpuThresholdMB * 1024 * 1024; // convert from MB
        if (mpuDisabled || obj.getMetadata().getContentLength() < thresholdSize) {
            Object data = new byte[0];
            if (!obj.isDirectory()) {
                data = obj.getInputStream();
                if (isMonitorPerformance())
                    data = new ProgressInputStream((InputStream) data, new S3WriteProgressListener());
            }

            final PutObjectRequest req = new PutObjectRequest(bucketName, targetKey, data).withObjectMetadata(om);

            if (includeAcl) req.setAcl(acl);

            PutObjectResult result = time(new Function<PutObjectResult>() {
                @Override
                public PutObjectResult call() {
                    return s3.putObject(req);
                }
            }, OPERATION_PUT_OBJECT);

            log.debug("Wrote {} etag: {}", targetKey, result.getETag());
        } else {
            LargeFileUploader uploader;

            // we can read file parts in parallel
            if (obj instanceof FileSyncObject) {
                uploader = new LargeFileUploader(s3, bucketName, targetKey, ((FileSyncObject) obj).getRawSourceIdentifier());
            } else {
                uploader = new LargeFileUploader(s3, bucketName, targetKey, obj.getInputStream(), obj.getMetadata().getContentLength());
            }
            if (isMonitorPerformance()) uploader.setProgressListener(new S3WriteProgressListener());
            uploader.withPartSize((long) mpuPartSizeMB * 1024 * 1024).withThreads(mpuThreadCount);
            uploader.setObjectMetadata(om);

            if (includeAcl) uploader.setAcl(acl);

            try {
                final LargeFileUploader fUploader = uploader;
                time(new Function<Void>() {
                    @Override
                    public Void call() {
                        fUploader.doMultipartUpload();
                        return null;
                    }
                }, OPERATION_MPU);
                log.debug("Wrote {} as MPU; etag: {}", targetKey, time(new Function<Object>() {
                    @Override
                    public Object call() {
                        return s3.getObjectMetadata(bucketName, targetKey).getETag();
                    }
                }, OPERATION_GET_METADATA));
            } finally {
                // make sure bytes read is accurate if we bypassed the counting stream
                if (obj instanceof FileSyncObject) {
                    try {
                        ((FileSyncObject) obj).setOverrideBytesRead(uploader.getBytesTransferred());
                    } catch (Throwable t) {
                        log.warn("could not get bytes transferred from upload", t);
                    }
                }
            }
        }
    }

    /**
     * Used to send transfer rate information up to the SyncPlugin
     */
    private class S3WriteProgressListener implements ProgressListener {

        @Override
        public void progress(long completed, long total) {
        }

        @Override
        public void transferred(long size) {
            if(getWritePerformanceCounter() != null) {
                getWritePerformanceCounter().increment(size);
            }
        }
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        obj.setTargetIdentifier(getTargetKey(obj));
        return new EcsS3SyncObject(this, s3, bucketName, getTargetKey(obj), obj.getRelativePath());
    }

    public ListIterator<EcsS3ObjectVersion> versionIterator(SyncObject obj) {
        return EcsS3Util.listVersions(this, s3, bucketName, getTargetKey(obj), obj.getRelativePath());
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
        return "Target that writes content to an ECS S3 bucket.  This " +
                "target plugin is triggered by the pattern:\n" +
                EcsS3Util.PATTERN_DESC + "\n" +
                "Scheme, host and port are all required. " +
                "root-prefix (optional) is the prefix to prepend to key names " +
                "when writing objects e.g. dir1/. If omitted, objects " +
                "will be written to the root of the bucket. Note that this plugin also " +
                "accepts the --force option to force overwriting target objects " +
                "even if they are the same or newer than the source.";
    }

    @Override
    public String summarizeConfig() {
        return super.summarizeConfig()
                + " - enableVHosts: " + enableVHosts + "\n"
                + " - smartClientEnabled: " + smartClientEnabled + "\n"
                + " - bucketName: " + bucketName + "\n"
                + " - createBucket: " + createBucket + "\n"
                + " - includeVersions: " + includeVersions + "\n"
                + " - apacheClientEnabled: " + apacheClientEnabled + "\n"
                + " - mpuDisabled: " + mpuDisabled + "\n"
                + " - mpuThresholdMB: " + mpuThresholdMB + "\n"
                + " - mpuPartSizeMB: " + mpuPartSizeMB + "\n"
                + " - mpuThreadCount: " + mpuThreadCount + "\n";
    }

    @Override
    public void cleanup() {
        s3.destroy();
        super.cleanup();
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public List<Vdc> getVdcs() {
        return vdcs;
    }

    public void setVdcs(List<Vdc> vdcs) {
        this.vdcs = vdcs;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public URI getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(URI endpoint) {
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

    public boolean isEnableVHosts() {
        return enableVHosts;
    }

    public void setEnableVHosts(boolean enableVHosts) {
        this.enableVHosts = enableVHosts;
    }

    public boolean isSmartClientEnabled() {
        return smartClientEnabled;
    }

    public void setSmartClientEnabled(boolean smartClientEnabled) {
        this.smartClientEnabled = smartClientEnabled;
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

    public boolean isApacheClientEnabled() {
        return apacheClientEnabled;
    }

    public void setApacheClientEnabled(boolean apacheClientEnabled) {
        this.apacheClientEnabled = apacheClientEnabled;
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

    public boolean isMpuDisabled() {
        return mpuDisabled;
    }

    public void setMpuDisabled(boolean mpuDisabled) {
        this.mpuDisabled = mpuDisabled;
    }

    public boolean isPreserveDirectories() {
        return preserveDirectories;
    }

    public void setPreserveDirectories(boolean preserveDirectories) {
        this.preserveDirectories = preserveDirectories;
    }
}

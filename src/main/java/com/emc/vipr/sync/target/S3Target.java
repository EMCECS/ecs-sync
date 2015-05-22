/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.target;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.object.S3ObjectVersion;
import com.emc.vipr.sync.model.object.S3SyncObject;
import com.emc.vipr.sync.model.object.SyncObject;
import com.emc.vipr.sync.source.S3Source;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.S3Util;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.util.*;

public class S3Target extends SyncTarget {
    private static final Logger l4j = Logger.getLogger(S3Target.class);

    public static final String BUCKET_OPTION = "target-bucket";
    public static final String BUCKET_DESC = "Required. Specifies the target bucket to use";
    public static final String BUCKET_ARG_NAME = "bucket";

    public static final String CREATE_BUCKET_OPTION = "target-create-bucket";
    public static final String CREATE_BUCKET_DESC = "By default, the target bucket must exist. This option will create it if it does not";

    public static final String DISABLE_VHOSTS_OPTION = "target-disable-vhost";
    public static final String DISABLE_VHOSTS_DESC = "If specified, virtual hosted buckets will be disabled and path-style buckets will be used.";

    private static final String INCLUDE_VERSIONS_OPTION = "s3-include-versions";
    private static final String INCLUDE_VERSIONS_DESC = "Transfer all versions of every object. NOTE: this will overwrite all versions of each source key in the target system if any exist!";

    private String protocol;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String bucketName;
    private String rootKey;
    private boolean disableVHosts;
    private boolean createBucket;
    private boolean includeVersions;
    private S3Source s3Source;

    private AmazonS3 s3;

    @Override
    public boolean canHandleTarget(String targetUri) {
        return targetUri.startsWith(S3Util.URI_PREFIX);
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(BUCKET_OPTION).withDescription(BUCKET_DESC)
                .hasArg().withArgName(BUCKET_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(DISABLE_VHOSTS_OPTION).withDescription(DISABLE_VHOSTS_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(CREATE_BUCKET_OPTION).withDescription(CREATE_BUCKET_DESC).create());
        opts.addOption(new OptionBuilder().withDescription(INCLUDE_VERSIONS_DESC).withLongOpt(INCLUDE_VERSIONS_OPTION).create());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        S3Util.S3Uri s3Uri = S3Util.parseUri(targetUri);
        protocol = s3Uri.protocol;
        endpoint = s3Uri.endpoint;
        accessKey = s3Uri.accessKey;
        secretKey = s3Uri.secretKey;
        rootKey = s3Uri.rootKey;

        if (line.hasOption(BUCKET_OPTION)) bucketName = line.getOptionValue(BUCKET_OPTION);

        disableVHosts = line.hasOption(DISABLE_VHOSTS_OPTION);

        createBucket = line.hasOption(CREATE_BUCKET_OPTION);

        includeVersions = line.hasOption(INCLUDE_VERSIONS_OPTION);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        Assert.hasText(accessKey, "accessKey is required");
        Assert.hasText(secretKey, "secretKey is required");
        Assert.hasText(bucketName, "bucketName is required");
        Assert.isTrue(bucketName.matches("[A-Za-z0-9._-]+"), bucketName + " is not a valid bucket name");

        AWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration config = new ClientConfiguration();

        if (protocol != null)
            config.setProtocol(Protocol.valueOf(protocol.toUpperCase()));

        s3 = new AmazonS3Client(creds, config);

        if (endpoint != null)
            s3.setEndpoint(endpoint);

        if (disableVHosts) {
            l4j.info("The use of virtual hosted buckets on the s3 source has been DISABLED.  Path style buckets will be used.");
            S3ClientOptions opts = new S3ClientOptions();
            opts.setPathStyleAccess(true);
            s3.setS3ClientOptions(opts);
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

        // for version support. TODO: genericize version support
        if (source instanceof S3Source) s3Source = (S3Source) source;
        if (includeVersions) {
            if (s3Source == null)
                throw new ConfigurationException("Object versions are currently only supported with the S3 source & target plugins.");
            String status = s3.getBucketVersioningConfiguration(bucketName).getStatus();
            if (BucketVersioningConfiguration.OFF.equals(status))
                throw new ConfigurationException("The specified bucket does not have versioning enabled.");
        }
    }

    @Override
    public void filter(SyncObject obj) {
        try {
            // S3 doesn't support directories.
            if (obj.isDirectory()) {
                l4j.debug("Skipping directory object in S3Target: " + obj.getRelativePath());
                return;
            }

            // some sync objects lazy-load their metadata (i.e. AtmosSyncObject)
            // since this may be a timed operation, ensure it loads outside of other timed operations
            if (!(obj instanceof S3ObjectVersion) || !((S3ObjectVersion) obj).isDeleteMarker())
                obj.getMetadata();

            // Compute target key
            String targetKey = getTargetKey(obj);
            obj.setTargetIdentifier(S3Util.fullPath(bucketName, targetKey));

            if (includeVersions) {
                ListIterator<S3ObjectVersion> sourceVersions = s3Source.versionIterator(obj);
                ListIterator<S3ObjectVersion> targetVersions = versionIterator(obj);

                boolean newVersions = false, replaceVersions = false;
                if (force) {
                    replaceVersions = true;
                } else {

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
                        l4j.info(String.format("Source and target versions are the same.  Skipping %s", obj.getRelativePath()));
                        return;
                    }
                }

                // something's off; must delete all versions of the object
                if (replaceVersions) {
                    LogMF.info(l4j, "[{0}]: version history differs between source and target; re-placing target version history with that from source.",
                            obj.getRelativePath());

                    // collect versions in target
                    List<DeleteObjectsRequest.KeyVersion> deleteVersions = new ArrayList<DeleteObjectsRequest.KeyVersion>();
                    while (targetVersions.hasNext()) targetVersions.next(); // move cursor to end
                    while (targetVersions.hasPrevious()) { // go in reverse order
                        S3ObjectVersion version = targetVersions.previous();
                        deleteVersions.add(new DeleteObjectsRequest.KeyVersion(targetKey, version.getVersionId()));
                    }

                    // batch delete all versions in target
                    LogMF.debug(l4j, "[{0}]: deleting all versions in target", obj.getRelativePath());
                    s3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(deleteVersions));

                    // replay version history in target
                    while (sourceVersions.hasPrevious()) sourceVersions.previous(); // move cursor to beginning
                    putIntermediateVersions(sourceVersions, targetKey);
                }

            } else { // normal sync (no versions)
                Date sourceLastModified = obj.getMetadata().getModificationTime();
                long sourceSize = obj.getMetadata().getSize();

                // Get target metadata.
                ObjectMetadata destMeta = null;
                try {
                    destMeta = s3.getObjectMetadata(bucketName, targetKey);
                } catch (AmazonS3Exception e) {
                    if (e.getStatusCode() == 404) {
                        // OK
                    } else {
                        throw new RuntimeException("Failed to check target key '" + targetKey + "' : " + e, e);
                    }
                }

                if (!force && destMeta != null) {

                    // Check overwrite
                    Date destLastModified = destMeta.getLastModified();
                    long destSize = destMeta.getContentLength();

                    if (destLastModified.equals(sourceLastModified) && sourceSize == destSize) {
                        l4j.info(String.format("Source and target the same.  Skipping %s", obj.getRelativePath()));
                        return;
                    }
                    if (destLastModified.after(sourceLastModified)) {
                        l4j.info(String.format("Target newer than source.  Skipping %s", obj.getRelativePath()));
                        return;
                    }
                }
            }

            // at this point we know we are going to write the object
            // Put [current object version]
            if (obj instanceof S3ObjectVersion && ((S3ObjectVersion) obj).isDeleteMarker()) {

                // object has version history, but is currently deleted
                LogMF.debug(l4j, "[{0}]: deleting object in target to replicate delete marker in source.", obj.getRelativePath());
                s3.deleteObject(bucketName, targetKey);
            } else {
                PutObjectResult resp = putObject(obj, targetKey);

                // if object has new metadata after the stream (i.e. encryption checksum), we must update S3 again
                if (obj.requiresPostStreamMetadataUpdate()) {
                    LogMF.debug(l4j, "[{0}]: updating metadata after sync as required", obj.getRelativePath());
                    CopyObjectRequest cReq = new CopyObjectRequest(bucketName, targetKey, bucketName, targetKey);
                    cReq.setNewObjectMetadata(S3Util.s3MetaFromSyncMeta(obj.getMetadata()));
                    s3.copyObject(cReq);
                }

                l4j.debug(String.format("Wrote %s etag: %s", targetKey, resp.getETag()));
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
                        LogMF.debug(l4j, "[{0}#{1}]: deleting object in target to replicate delete marker in source.",
                                version.getRelativePath(), version.getVersionId());
                        s3.deleteObject(bucketName, key);
                    } else {
                        if (version.getVersionId() == null || version.getVersionId().equals("null")) { // workaround for STORAGE-6784
                            LogMF.warn(l4j, "[{0}#{1}]: source versionId is null, assuming STORAGE-6784 is cause; writing placeholder instead",
                                    version.getRelativePath(), version.getVersionId());
                            String message = "vipr-sync: original source version lost due to bug STORAGE-6784. this is a placeholder";
                            ObjectMetadata om = new ObjectMetadata();
                            om.setContentLength(message.length()); // to hush warnings from AWS SDK
                            s3.putObject(bucketName, key, new ByteArrayInputStream(message.getBytes()), om);
                        } else {
                            LogMF.debug(l4j, "[{0}#{1}]: replicating historical version in target.",
                                    version.getRelativePath(), version.getVersionId());
                            putObject(version, key);
                        }
                    }
                }
            } catch (RuntimeException e) {
                throw new RuntimeException(String.format("sync of historical version %s failed", version.getVersionId()), e);
            }
        }
    }

    protected PutObjectResult putObject(SyncObject obj, String targetKey) {
        ObjectMetadata om = S3Util.s3MetaFromSyncMeta(obj.getMetadata());
        PutObjectRequest req = new PutObjectRequest(bucketName, targetKey, obj.getInputStream(), om);

        if (includeAcl)
            req.setAccessControlList(S3Util.s3AclFromSyncAcl(obj.getMetadata().getAcl(), ignoreInvalidAcls));

        return s3.putObject(req);
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        return new S3SyncObject(this, s3, bucketName, getTargetKey(obj), obj.getRelativePath(), obj.isDirectory());
    }

    public ListIterator<S3ObjectVersion> versionIterator(SyncObject obj) {
        if (obj.isDirectory()) {
            return null;
        } else {
            return S3Util.listVersions(this, s3, bucketName, getTargetKey(obj), obj.getRelativePath());
        }
    }

    private String getTargetKey(SyncObject obj) {
        return rootKey + obj.getRelativePath();
    }

    @Override
    public String getName() {
        return "S3 Target";
    }

    @Override
    public String getDocumentation() {
        return "Target that writes content to an S3 bucket.  This " +
                "target plugin is triggered by the pattern:\n" +
                S3Util.PATTERN_DESC + "\n" +
                "Scheme, host and port are all optional. If ommitted, " +
                "https://s3.amazonaws.com:443 is assumed. " +
                "root-prefix (optional) is the prefix to prepend to key names " +
                "when writing objects e.g. dir1/. If omitted, objects " +
                "will be written to the root of the bucket. Note that this plugin also " +
                "accepts the --force option to force overwriting target objects " +
                "even if they are the same or newer than the source.";
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
}

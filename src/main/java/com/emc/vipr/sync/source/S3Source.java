/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.source;

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
import com.emc.vipr.sync.target.S3Target;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * This class implements an Amazon Simple Storage Service (S3) source for data.
 *
 * @author cwikj
 */
public class S3Source extends SyncSource<S3SyncObject> {
    private static final Logger l4j = Logger.getLogger(S3Source.class);

    public static final String BUCKET_OPTION = "source-bucket";
    public static final String BUCKET_DESC = "Required. Specifies the source bucket to use.";
    public static final String BUCKET_ARG_NAME = "bucket";

    public static final String DECODE_KEYS_OPTION = "source-decode-keys";
    public static final String DECODE_KEYS_DESC = "If specified, keys will be URL-decoded after listing them.  This can fix problems if you see file or directory names with characters like %2f in them.";

    public static final String DISABLE_VHOSTS_OPTION = "source-disable-vhost";
    public static final String DISABLE_VHOSTS_DESC = "If specified, virtual hosted buckets will be disabled and path-style buckets will be used.";

    public static final String OPERATION_DELETE_OBJECT = "S3DeleteObject";

    private String protocol;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private boolean disableVHosts;
    private String bucketName;
    private String rootKey;
    private boolean decodeKeys;
    private S3Target s3Target;
    private boolean versioningEnabled = false;

    private AmazonS3 s3;

    @Override
    public boolean canHandleSource(String sourceUri) {
        return sourceUri.startsWith(S3Util.URI_PREFIX);
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(BUCKET_OPTION).withDescription(BUCKET_DESC)
                .hasArg().withArgName(BUCKET_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(DECODE_KEYS_OPTION).withDescription(DECODE_KEYS_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(DISABLE_VHOSTS_OPTION).withDescription(DISABLE_VHOSTS_DESC).create());
        return opts;
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        S3Util.S3Uri s3Uri = S3Util.parseUri(sourceUri);
        protocol = s3Uri.protocol;
        endpoint = s3Uri.endpoint;
        accessKey = s3Uri.accessKey;
        secretKey = s3Uri.secretKey;
        rootKey = s3Uri.rootKey;

        if (line.hasOption(BUCKET_OPTION))
            bucketName = line.getOptionValue(BUCKET_OPTION);

        disableVHosts = line.hasOption(DISABLE_VHOSTS_OPTION);

        decodeKeys = line.hasOption(DECODE_KEYS_OPTION);
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

        // TODO: generalize uri translation
        S3Util.S3Uri s3Uri = new S3Util.S3Uri();
        s3Uri.protocol = protocol;
        s3Uri.endpoint = endpoint;
        s3Uri.accessKey = accessKey;
        s3Uri.secretKey = secretKey;
        s3Uri.rootKey = rootKey;
        if (sourceUri == null) sourceUri = s3Uri.toUri();

        if (disableVHosts) {
            l4j.info("The use of virtual hosted buckets on the s3 source has been DISABLED.  Path style buckets will be used.");
            S3ClientOptions opts = new S3ClientOptions();
            opts.setPathStyleAccess(true);
            s3.setS3ClientOptions(opts);
        }

        if (!s3.doesBucketExist(bucketName)) {
            throw new ConfigurationException("The bucket " + bucketName + " does not exist.");
        }

        if (rootKey == null) rootKey = ""; // make sure rootKey isn't null

        // for version support. TODO: genericize version support
        if (target instanceof S3Target) {
            s3Target = (S3Target) target;
            if (s3Target.isIncludeVersions()) {
                BucketVersioningConfiguration versioningConfig = s3.getBucketVersioningConfiguration(bucketName);
                List<String> versionedStates = Arrays.asList(BucketVersioningConfiguration.ENABLED, BucketVersioningConfiguration.SUSPENDED);
                versioningEnabled = versionedStates.contains(versioningConfig.getStatus());
            }
        }
    }

    @Override
    public Iterator<S3SyncObject> iterator() {
        // root key ending in a slash signifies a directory
        if (rootKey.isEmpty() || rootKey.endsWith("/")) {
            if (s3Target != null && s3Target.isIncludeVersions()) {
                return new CombinedIterator<S3SyncObject>(new PrefixIterator(rootKey), new DeletedObjectIterator(rootKey));
            } else {
                return new PrefixIterator(rootKey);
            }
        } else { // otherwise, assume only one object
            return Arrays.asList(new S3SyncObject(this, s3, bucketName, rootKey, getRelativePath(rootKey), false)).iterator();
        }
    }

    @Override
    public Iterator<S3SyncObject> childIterator(S3SyncObject syncObject) {
        if (syncObject.isDirectory()) {
            return new PrefixIterator(syncObject.getKey());
        } else {
            return null;
        }
    }

    /**
     * To support versions. Called by S3Target to sync all versions of an object
     */
    public ListIterator<S3ObjectVersion> versionIterator(SyncObject syncObject) {
        if (syncObject.isDirectory()) {
            return null;
        } else {
            String key = rootKey + syncObject.getRelativePath();
            return S3Util.listVersions(this, s3, bucketName, key, syncObject.getRelativePath());
        }
    }

    /**
     * Overridden to support versions
     */
    @Override
    public void verify(S3SyncObject syncObject, SyncFilter filterChain) {

        // this implementation only verifies data objects
        if (syncObject.isDirectory()) return;

        // must first verify versions
        if (s3Target != null && s3Target.isIncludeVersions()) {
            Iterator<S3ObjectVersion> sourceVersions = versionIterator(syncObject);
            Iterator<S3ObjectVersion> targetVersions = s3Target.versionIterator(syncObject);

            // special workaround for bug where objects are listed, but they have no versions
            if (sourceVersions.hasNext()) {

                while (sourceVersions.hasNext()) {
                    if (!targetVersions.hasNext())
                        throw new RuntimeException("The source system has more versions of the object than the target");

                    S3ObjectVersion sourceVersion = sourceVersions.next();
                    S3ObjectVersion targetVersion = targetVersions.next();

                    if (sourceVersion.isLatest()) continue; // current version is verified through filter chain below

                    LogMF.debug(l4j, "#==? verifying version (source vID: {0}, target vID: {1})",
                            sourceVersion.getVersionId(), targetVersion.getVersionId());

                    if (sourceVersion.isDeleteMarker()) {
                        if (targetVersion.isDeleteMarker()) {
                            LogMF.info(l4j, "#==# delete marker verified for version (source vID: {0}, target vID: {1})",
                                    sourceVersion.getVersionId(), targetVersion.getVersionId());
                        } else {
                            throw new RuntimeException(String.format("Version: source is delete marker; target isn't (source vID: %s, target vID: %s)",
                                    sourceVersion.getVersionId(), targetVersion.getVersionId()));
                        }

                    } else {
                        if (targetVersion.isDeleteMarker()) {
                            throw new RuntimeException(String.format("Version: target is delete marker; source isn't (source vID: %s, target vID: %s)",
                                    sourceVersion.getVersionId(), targetVersion.getVersionId()));
                        }

                        try {
                            verifyObjects(sourceVersion, targetVersion);
                            LogMF.info(l4j, "#==# checksum verified for version (source vID: {0}, target vID: {1})",
                                    sourceVersion.getVersionId(), targetVersion.getVersionId());
                        } catch (RuntimeException e) {
                            throw new RuntimeException(String.format("Version: checksum failed (source vID: %s, target vID: %s)",
                                    sourceVersion.getVersionId(), targetVersion.getVersionId()), e);
                        }
                    }
                }

                if (targetVersions.hasNext()) {
                    throw new RuntimeException(String.format("The target system has more versions of the object than the source (are other clients writing to the target?) [{%s}]",
                            targetVersions.next().getSourceIdentifier()));
                }
            }
        }

        // verify current version
        if (syncObject instanceof S3ObjectVersion && ((S3ObjectVersion) syncObject).isDeleteMarker()) {
            SyncObject targetObject = filterChain.reverseFilter(syncObject);
            try {
                targetObject.getMetadata(); // this should return a 404
                throw new RuntimeException("Latest object version is a delete marker on the source, but exists on the target");
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() != 404) throw e; // if it's not a 404, there was some other error
            }
        } else {
            super.verify(syncObject, filterChain);
        }
    }

    @Override
    public void delete(final S3SyncObject syncObject) {
        if (!syncObject.isDirectory()) {
            time(new Function<Void>() {
                @Override
                public Void call() {
                    s3.deleteObject(bucketName, syncObject.getKey());
                    return null;
                }
            }, OPERATION_DELETE_OBJECT);
        }
    }

    @Override
    public String getName() {
        return "S3 Source";
    }

    @Override
    public String getDocumentation() {
        return "Scans and reads content from an Amazon S3 bucket. This " +
                "source plugin is triggered by the pattern:\n" +
                S3Util.PATTERN_DESC + "\n" +
                "Scheme, host and port are all optional. If ommitted, " +
                "https://s3.amazonaws.com:443 is assumed. " +
                "root-prefix (optional) is the prefix under which to start " +
                "enumerating within the bucket, e.g. dir1/. If omitted the " +
                "root of the bucket will be enumerated.";
    }

    protected String getRelativePath(String key) {
        if (key.startsWith(rootKey)) key = key.substring(rootKey.length());
        return decodeKeys ? decodeKey(key) : key;
    }

    protected String decodeKey(String key) {
        try {
            return URLDecoder.decode(key, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 is not supported on this platform");
        }
    }

    protected class PrefixIterator extends ReadOnlyIterator<S3SyncObject> {
        private String key;
        private ObjectListing listing;
        private Iterator<S3ObjectSummary> objectIterator;
        private Iterator<String> prefixIterator;

        public PrefixIterator(String key) {
            this.key = key;
        }

        @Override
        protected S3SyncObject getNextObject() {
            if (listing == null || (!objectIterator.hasNext() && !prefixIterator.hasNext() && listing.isTruncated())) {
                getNextBatch();
            }

            if (objectIterator.hasNext()) {
                S3ObjectSummary summary = objectIterator.next();
                return new S3SyncObject(S3Source.this, s3, bucketName, summary.getKey(), getRelativePath(summary.getKey()), false);
            }
            if (prefixIterator.hasNext()) {
                String prefix = prefixIterator.next();
                return new S3SyncObject(S3Source.this, s3, bucketName, prefix, getRelativePath(prefix), true);
            }

            // list is not truncated and iterators are finished; no more objects
            return null;
        }

        private void getNextBatch() {
            if (listing == null) {
                if ("".equals(key)) {
                    // The user has specified the root key, meaning that they want to copy the entire bucket.  In this
                    // scenario, we can list the bucket in "flat" mode without bothering about prefixes.
                    l4j.info("Enumerating bucket in flat mode");
                    listing = s3.listObjects(new ListObjectsRequest(bucketName, null, null, null, 1000));
                } else {
                    listing = s3.listObjects(new ListObjectsRequest(bucketName, key, null, "/", 1000));
                }
            } else {
                listing = s3.listNextBatchOfObjects(listing);
            }
            objectIterator = listing.getObjectSummaries().iterator();
            prefixIterator = listing.getCommonPrefixes().iterator();
        }
    }

    protected class DeletedObjectIterator extends ReadOnlyIterator<S3SyncObject> {
        private String rootKey;
        private VersionListing versionListing;
        private Iterator<S3VersionSummary> versionIterator;

        public DeletedObjectIterator(String rootKey) {
            this.rootKey = rootKey;
        }

        @Override
        protected S3SyncObject getNextObject() {
            while (true) {
                S3VersionSummary versionSummary = getNextSummary();

                if (versionSummary == null) return null;

                if (versionSummary.isLatest() && versionSummary.isDeleteMarker())
                    return new S3ObjectVersion(S3Source.this, s3, bucketName, versionSummary.getKey(),
                            versionSummary.getVersionId(), versionSummary.isLatest(),
                            versionSummary.isDeleteMarker(), versionSummary.getLastModified(),
                            versionSummary.getETag(), getRelativePath(versionSummary.getKey()));
            }
        }

        protected S3VersionSummary getNextSummary() {
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
                versionListing = s3.listVersions(bucketName, rootKey == null || rootKey.length() == 0 ? null : rootKey);
            } else {
                versionListing = s3.listNextBatchOfVersions(versionListing);
            }
            versionIterator = versionListing.getVersionSummaries().iterator();
        }
    }

    protected class CombinedIterator<T> extends ReadOnlyIterator<T> {
        private Iterator<T>[] iterators;
        private int currentIterator = 0;

        public CombinedIterator(Iterator<T>... iterators) {
            this.iterators = iterators;
        }

        @Override
        protected T getNextObject() {
            while (currentIterator < iterators.length) {
                if (iterators[currentIterator].hasNext()) return iterators[currentIterator].next();
                currentIterator++;
            }

            return null;
        }
    }

    /**
     * @return the bucketName
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * @param bucketName the bucketName to set
     */
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    /**
     * @return the rootKey
     */
    public String getRootKey() {
        return rootKey;
    }

    /**
     * @param rootKey the rootKey to set
     */
    public void setRootKey(String rootKey) {
        this.rootKey = rootKey;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    /**
     * @return the endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * @param endpoint the endpoint to set
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * @return the accessKey
     */
    public String getAccessKey() {
        return accessKey;
    }

    /**
     * @param accessKey the accessKey to set
     */
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    /**
     * @return the secretKey
     */
    public String getSecretKey() {
        return secretKey;
    }

    /**
     * @param secretKey the secretKey to set
     */
    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    /**
     * @return the decodeKeys
     */
    public boolean isDecodeKeys() {
        return decodeKeys;
    }

    /**
     * @param decodeKeys the decodeKeys to set
     */
    public void setDecodeKeys(boolean decodeKeys) {
        this.decodeKeys = decodeKeys;
    }

    public boolean isVersioningEnabled() {
        return versioningEnabled;
    }
}

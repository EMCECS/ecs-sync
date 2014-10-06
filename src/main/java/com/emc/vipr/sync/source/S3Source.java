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
import com.emc.vipr.sync.model.Checksum;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class implements an Amazon Simple Storage Service (S3) source for data.
 *
 * @author cwikj
 */
public class S3Source extends SyncSource<S3Source.S3SyncObject> {
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
            throw new ConfigurationException("The bucket " + bucketName + " does not exist.");
        }

        if (rootKey == null) rootKey = ""; // make sure rootKey isn't null
        if (rootKey.startsWith("/")) rootKey = rootKey.substring(1); // " " does not start with slash
    }

    @Override
    public Iterator<S3SyncObject> iterator() {
        // root key ending in a slash signifies a directory
        if (rootKey.isEmpty() || rootKey.endsWith("/")) return new PrefixIterator(rootKey);

            // otherwise, assume only one object
        else return Arrays.asList(new S3SyncObject(rootKey, getRelativePath(rootKey), false)).iterator();
    }

    @Override
    public Iterator<S3SyncObject> childIterator(S3SyncObject syncObject) {
        if (syncObject.isDirectory()) {
            return new PrefixIterator(syncObject.getSourceIdentifier());
        } else {
            return null;
        }
    }

    @Override
    public void delete(final S3SyncObject syncObject) {
        if (!syncObject.isDirectory()) {
            time(new Timeable<Void>() {
                @Override
                public Void call() {
                    s3.deleteObject(bucketName, syncObject.getSourceIdentifier());
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
        if (key.startsWith(rootKey)) return key.substring(rootKey.length());
        return key;
    }

    protected String decodeKey(String key) {
        try {
            return URLDecoder.decode(key, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 is not supported on this platform");
        }
    }

    private Map<String, SyncMetadata.UserMetadata> toMetaMap(Map<String, String> sourceMap) {
        Map<String, SyncMetadata.UserMetadata> metaMap = new HashMap<>();
        for (String key : sourceMap.keySet()) {
            metaMap.put(key, new SyncMetadata.UserMetadata(key, sourceMap.get(key)));
        }
        return metaMap;
    }

    public class S3SyncObject extends SyncObject<String> {
        private S3Object object;

        public S3SyncObject(String key, String relativePath, boolean isCommonPrefix) {
            super(key, key, decodeKeys ? decodeKey(relativePath) : relativePath, isCommonPrefix);
        }

        @Override
        public InputStream createSourceInputStream() {
            if (isDirectory()) return null;
            checkLoaded();
            return new BufferedInputStream(object.getObjectContent(), bufferSize);
        }

        @Override
        protected void loadObject() {
            if (isDirectory()) return;

            object = s3.getObject(bucketName, sourceIdentifier);

            // load metadata
            ObjectMetadata s3meta = object.getObjectMetadata();
            SyncMetadata meta = new SyncMetadata();

            meta.setChecksum(new Checksum("MD5", s3meta.getContentMD5()));
            meta.setContentType(s3meta.getContentType());
            meta.setExpirationDate(s3meta.getExpirationTime());
            meta.setModificationTime(s3meta.getLastModified());
            meta.setSize(s3meta.getContentLength());
            meta.setUserMetadata(toMetaMap(s3meta.getUserMetadata()));

            if (includeAcl) {
                meta.setAcl(S3Util.syncAclFromS3Acl(s3.getObjectAcl(bucketName, sourceIdentifier)));
            }

            metadata = meta;
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
                return new S3SyncObject(summary.getKey(), getRelativePath(summary.getKey()), false);
            }
            if (prefixIterator.hasNext()) {
                String prefix = prefixIterator.next();
                return new S3SyncObject(prefix, getRelativePath(prefix), true);
            }

            // list is not truncated and iterators are finished; no more objects
            return null;
        }

        private void getNextBatch() {
            if (listing == null) {
                listing = s3.listObjects(new ListObjectsRequest(bucketName, key, null, "/", 1000));
            } else {
                listing = s3.listNextBatchOfObjects(listing);
            }
            objectIterator = listing.getObjectSummaries().iterator();
            prefixIterator = listing.getCommonPrefixes().iterator();
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
}

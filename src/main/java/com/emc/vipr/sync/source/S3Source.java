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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.BasicMetadata;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.ReadOnlyIterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Iterator;

/**
 * This class implements an Amazon Simple Storage Service (S3) source for data.
 *
 * @author cwikj
 */
public class S3Source extends SyncSource<S3Source.S3SyncObject> {
    private static final Logger l4j = Logger.getLogger(S3Source.class);

    public static final String SOURCE_PREFIX = "s3:";

    public static final String ACCESS_KEY_OPTION = "s3-source-access-key";
    public static final String ACCESS_KEY_DESC = "The Amazon S3 access key, e.g. 0PN5J17HBGZHT7JJ3X82";
    public static final String ACCESS_KEY_ARG_NAME = "access-key";

    public static final String SECRET_KEY_OPTION = "s3-source-secret-key";
    public static final String SECRET_KEY_DESC = "The Amazon S3 secret key, e.g. uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o";
    public static final String SECRET_KEY_ARG_NAME = "secret-key";

    public static final String ROOT_KEY_OPTION = "s3-source-root-key";
    public static final String ROOT_KEY_DESC = "The key to start enumerating within the bucket, e.g. dir1/.  Optional, if omitted the root of the bucket will be enumerated.";
    public static final String ROOT_KEY_ARG_NAME = "root-key";

    public static final String ENDPOINT_OPTION = "s3-source-endpoint";
    public static final String ENDPOINT_DESC = "Specifies a different endpoint. If not set, s3.amazonaws.com is assumed.";
    public static final String ENDPOINT_ARG_NAME = "endpoint";

    public static final String DECODE_KEYS_OPTION = "s3-source-decode-keys";
    public static final String DECODE_KEYS_DESC = "If specified, keys will be URL-decoded after listing them.  This can fix problems if you see file or directory names with characters like %2f in them.";

    public static final String DISABLE_VHOSTS_OPTION = "s3-source-disable-vhost";
    public static final String DISABLE_VHOSTS_DESC = "If specified, virtual hosted buckets will be disabled and path-style buckets will be used.";

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
        return sourceUri.startsWith(SOURCE_PREFIX);
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(ACCESS_KEY_OPTION).withDescription(ACCESS_KEY_DESC)
                .hasArg().withArgName(ACCESS_KEY_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(SECRET_KEY_OPTION).withDescription(SECRET_KEY_DESC)
                .hasArg().withArgName(SECRET_KEY_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(ROOT_KEY_OPTION).withDescription(ROOT_KEY_DESC)
                .hasArg().withArgName(ROOT_KEY_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(ENDPOINT_OPTION).withDescription(ENDPOINT_DESC)
                .hasArg().withArgName(ENDPOINT_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(DECODE_KEYS_OPTION).withDescription(DECODE_KEYS_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(DISABLE_VHOSTS_OPTION).withDescription(DISABLE_VHOSTS_DESC).create());
        return opts;
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        if (!sourceUri.startsWith(SOURCE_PREFIX))
            throw new ConfigurationException("source must start with " + SOURCE_PREFIX);

        if (line.hasOption(ENDPOINT_OPTION))
            endpoint = line.getOptionValue(ENDPOINT_OPTION);

        if (line.hasOption(ACCESS_KEY_OPTION))
            accessKey = line.getOptionValue(ACCESS_KEY_OPTION);

        if (line.hasOption(SECRET_KEY_OPTION))
            secretKey = line.getOptionValue(SECRET_KEY_OPTION);

        bucketName = sourceUri.substring(3);

        if (line.hasOption(ROOT_KEY_OPTION))
            rootKey = line.getOptionValue(ROOT_KEY_OPTION);

        disableVHosts = line.hasOption(DISABLE_VHOSTS_OPTION);

        decodeKeys = line.hasOption(DECODE_KEYS_OPTION);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        Assert.hasText(accessKey, "accessKey is required");
        Assert.hasText(secretKey, "secretKey is required");
        Assert.hasText(bucketName, "bucketName is required");

        AWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
        s3 = new AmazonS3Client(creds);

        if (endpoint != null) {
            s3.setEndpoint(endpoint);
        }

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
        if (syncObject.hasChildren()) {
            return new PrefixIterator(syncObject.getSourceIdentifier());
        } else {
            return null;
        }
    }

    @Override
    public void delete(S3SyncObject syncObject) {
        if (syncObject.hasData()) {
            s3.deleteObject(bucketName, syncObject.getSourceIdentifier());
        }
    }

    @Override
    public String getName() {
        return "S3 Source";
    }

    @Override
    public String getDocumentation() {
        return "Scans and reads content from an Amazon S3 bucket.  This "
                + "source plugin is triggered by the pattern:\n"
                + "s3:<bucket>\n" + "e.g. s3:mybucket";
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

    public class S3SyncObject extends SyncObject<S3SyncObject> {
        private boolean isCommonPrefix;
        private S3Object object;

        public S3SyncObject(String key, String relativePath, boolean isCommonPrefix) {
            super(key, decodeKeys ? decodeKey(relativePath) : relativePath);
            this.isCommonPrefix = isCommonPrefix;
        }

        @Override
        public Object getRawSourceIdentifier() {
            return sourceIdentifier;
        }

        @Override
        public boolean hasData() {
            return !isCommonPrefix;
        }

        @Override
        public long getSize() {
            if (!hasData()) return 0;
            loadObject();
            return object.getObjectMetadata().getContentLength();
        }

        @Override
        public InputStream createSourceInputStream() {
            if (!hasData()) return new ByteArrayInputStream(new byte[]{});
            loadObject();
            return new BufferedInputStream(object.getObjectContent(), bufferSize);
        }

        @Override
        public boolean hasChildren() {
            return isCommonPrefix;
        }

        @Override
        public SyncMetadata getMetadata() {
            if (hasData()) loadObject();
            return super.getMetadata();
        }

        private void loadObject() {
            if (object != null) return;
            synchronized (this) {
                if (object != null) return;
                object = s3.getObject(bucketName, sourceIdentifier);

                // load metadata
                ObjectMetadata s3meta = object.getObjectMetadata();
                BasicMetadata meta = new BasicMetadata();

                meta.setContentType(s3meta.getContentType());
                meta.setModifiedTime(s3meta.getLastModified());
                meta.setUserMetadata(s3meta.getUserMetadata());
                meta.getSystemMetadata().put("size", "" + s3meta.getContentLength());

                metadata = meta;
            }
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

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
package com.emc.ecs.sync.source;

import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.SyncEstimate;
import com.emc.ecs.sync.model.object.EcsS3ObjectVersion;
import com.emc.ecs.sync.model.object.EcsS3SyncObject;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.target.EcsS3Target;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.*;
import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.bean.*;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.rest.smart.ecs.Vdc;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.*;

public class EcsS3Source extends SyncSource<EcsS3SyncObject> {
    private static final Logger log = LoggerFactory.getLogger(EcsS3Source.class);

    public static final String BUCKET_OPTION = "source-bucket";
    public static final String BUCKET_DESC = "Required. Specifies the source bucket to use.";
    public static final String BUCKET_ARG_NAME = "bucket";

    public static final String DECODE_KEYS_OPTION = "source-decode-keys";
    public static final String DECODE_KEYS_DESC = "If specified, keys will be URL-decoded after listing them.  This can fix problems if you see file or directory names with characters like %2f in them.";

    public static final String ENABLE_VHOSTS_OPTION = "source-enable-vhost";
    public static final String ENABLE_VHOSTS_DESC = "If specified, virtual hosted buckets will be enabled (bucket.s3.company.com). This will also disable node discovery.";

    public static final String NO_SMART_CLIENT_OPTION = "source-no-smart-client";
    public static final String NO_SMART_CLIENT_DESC = "Disables the smart client (client-side load balancing). Necessary when using a proxy or external load balancer without DNS configuration.";

    public static final String GEO_PINNING_OPTION = "source-geo-pinning";
    public static final String GEO_PINNING_DESC = "Enables geo-pinning on the source (do not enable unless you know the data has been geo-pinned)";

    public static final String APACHE_CLIENT_OPTION = "source-apache-client";
    public static final String APACHE_CLIENT_DESC = "If specified, source will use the Apache HTTP client, which is not as efficient, but enables Expect: 100-Continue (header pre-flight).";

    public static final String OPERATION_DELETE_OBJECT = "EcsS3DeleteObject";

    public static final String SOURCE_KEY_LIST_OPTION = "source-key-list";
    public static final String SOURCE_KEY_LIST_DESC = "If specified, the list of keys to transfer will be read from the named file.";

    private String protocol;
    private List<Vdc> vdcs;
    private int port;
    private URI endpoint;
    private String accessKey;
    private String secretKey;
    private boolean enableVHosts;
    private boolean smartClientEnabled = true;
    private boolean geoPinningEnabled;
    private String bucketName;
    private String rootKey;
    private boolean decodeKeys;
    private EcsS3Target s3Target;
    private boolean versioningEnabled;
    private boolean apacheClientEnabled;
    private File sourceKeyList;

    private S3Client s3;

    @Override
    public boolean canHandleSource(String sourceUri) {
        return sourceUri.startsWith(EcsS3Util.URI_PREFIX);
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt(BUCKET_OPTION).desc(BUCKET_DESC)
                .hasArg().argName(BUCKET_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(DECODE_KEYS_OPTION).desc(DECODE_KEYS_DESC).build());
        opts.addOption(Option.builder().longOpt(ENABLE_VHOSTS_OPTION).desc(ENABLE_VHOSTS_DESC).build());
        opts.addOption(Option.builder().longOpt(NO_SMART_CLIENT_OPTION).desc(NO_SMART_CLIENT_DESC).build());
        opts.addOption(Option.builder().longOpt(GEO_PINNING_OPTION).desc(GEO_PINNING_DESC).build());
        opts.addOption(Option.builder().longOpt(APACHE_CLIENT_OPTION).desc(APACHE_CLIENT_DESC).build());
        opts.addOption(Option.builder().longOpt(SOURCE_KEY_LIST_OPTION)
                .hasArg().argName("filename").desc(SOURCE_KEY_LIST_DESC).build());
        return opts;
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        EcsS3Util.S3Uri s3Uri = EcsS3Util.parseUri(sourceUri);
        protocol = s3Uri.protocol;
        vdcs = s3Uri.vdcs;
        port = s3Uri.port;
        accessKey = s3Uri.accessKey;
        secretKey = s3Uri.secretKey;
        rootKey = s3Uri.rootKey;
        endpoint = s3Uri.getEndpointUri();

        if (line.hasOption(BUCKET_OPTION))
            bucketName = line.getOptionValue(BUCKET_OPTION);

        decodeKeys = line.hasOption(DECODE_KEYS_OPTION);

        enableVHosts = line.hasOption(ENABLE_VHOSTS_OPTION);

        smartClientEnabled = !line.hasOption(NO_SMART_CLIENT_OPTION);

        geoPinningEnabled = line.hasOption(GEO_PINNING_OPTION);

        apacheClientEnabled = line.hasOption(APACHE_CLIENT_OPTION);

        if(line.hasOption(SOURCE_KEY_LIST_OPTION)) {
            sourceKeyList = new File(line.getOptionValue(SOURCE_KEY_LIST_OPTION));
        }
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
            s3Config = new S3Config(Protocol.valueOf(protocol.toUpperCase()), vdcs.toArray(new Vdc[vdcs.size()]));
            if (port > 0) s3Config.setPort(port);
            s3Config.setSmartClient(smartClientEnabled);
        }
        s3Config.withIdentity(accessKey).withSecretKey(secretKey);

        if (geoPinningEnabled) {
            if (s3Config.getVdcs() == null || s3Config.getVdcs().size() < 3)
                throw new ConfigurationException("geo-pinning should only be enabled for 3+ VDCs!");
            s3Config.setGeoPinningEnabled(true);
        }

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
        if (sourceUri == null) sourceUri = s3Uri.toUri();

        if (!s3.bucketExists(bucketName)) {
            throw new ConfigurationException("The bucket " + bucketName + " does not exist.");
        }

        if (rootKey == null) rootKey = ""; // make sure rootKey isn't null

        // for version support. TODO: genericize version support
        if (target instanceof EcsS3Target) {
            s3Target = (EcsS3Target) target;
            if (s3Target.isIncludeVersions()) {
                VersioningConfiguration versioningConfig = s3.getBucketVersioning(bucketName);
                List<VersioningConfiguration.Status> versionedStates = Arrays.asList(
                        VersioningConfiguration.Status.Enabled, VersioningConfiguration.Status.Suspended);
                versioningEnabled = versionedStates.contains(versioningConfig.getStatus());
            }
        }

        if(sourceKeyList != null) {
            if(!sourceKeyList.exists()) {
                throw new ConfigurationException("The key list file " + sourceKeyList + " does not exist");
            }
        }
    }

    @Override
    public SyncEstimate createEstimate() {
        SyncEstimate estimate = new SyncEstimate();

        if(sourceKeyList != null) {
            Iterator<EcsS3SyncObject> i = getSourceKeyListIterator();
            while (i.hasNext() && i.next() != null) estimate.incTotalObjectCount(1);
            return estimate;
        }

        // root key ending in a slash signifies a directory
        if (rootKey.isEmpty() || rootKey.endsWith("/")) {
            ListObjectsResult listResult = null;
            do {
                if (listResult == null) listResult = s3.listObjects(bucketName, rootKey);
                else listResult = s3.listMoreObjects(listResult);

                for (S3Object object : listResult.getObjects()) {
                    estimate.incTotalObjectCount(1);
                    estimate.incTotalByteCount(object.getSize());
                }
            } while (listResult.isTruncated());

        } else { // otherwise, assume only one object
            estimate.incTotalObjectCount(1);
            estimate.incTotalByteCount(s3.getObjectMetadata(bucketName, rootKey).getContentLength());
        }

        return estimate;
    }

    @Override
    public Iterator<EcsS3SyncObject> iterator() {
        if(sourceKeyList != null) {
            return getSourceKeyListIterator();
        }

        // root key ending in a slash signifies a directory
        if (rootKey.isEmpty() || rootKey.endsWith("/")) {
            if (versioningEnabled) {
                return new CombinedIterator<>(Arrays.asList(new PrefixIterator(rootKey), new DeletedObjectIterator(rootKey)));
            } else {
                return new PrefixIterator(rootKey);
            }
        } else { // otherwise, assume only one object
            return Collections.singletonList(new EcsS3SyncObject(this, s3, bucketName, rootKey, getRelativePath(rootKey))).iterator();
        }
    }

    /**
     * Enumerates the keys to transfer from a flat list file
     */
    private Iterator<EcsS3SyncObject> getSourceKeyListIterator() {
        final Iterator<String> fileIterator = new FileLineIterator(sourceKeyList);

        return new ReadOnlyIterator<EcsS3SyncObject>() {
            @Override
            protected EcsS3SyncObject getNextObject() {
                if (fileIterator.hasNext()) {
                    String key = fileIterator.next();
                    return new EcsS3SyncObject(EcsS3Source.this, s3, bucketName, key, getRelativePath(key));
                }
                return null;
            }
        };
    }


    /**
     * This source is designed to query all objects under the root prefix as a flat list of results (no hierarchy),
     * which means <strong>no</strong> objects should have children
     */
    @Override
    public Iterator<EcsS3SyncObject> childIterator(EcsS3SyncObject syncObject) {
        return Collections.emptyIterator();
    }

    /**
     * To support versions. Called by S3Target to sync all versions of an object
     */
    public ListIterator<EcsS3ObjectVersion> versionIterator(EcsS3SyncObject syncObject) {
        return EcsS3Util.listVersions(this, s3, bucketName, syncObject.getKey(), syncObject.getRelativePath());
    }

    /**
     * Overridden to support versions
     */
    @Override
    public void verify(EcsS3SyncObject syncObject, SyncFilter filterChain) {

        // this implementation only verifies data objects
        if (syncObject.isDirectory()) return;

        // must first verify versions
        if (s3Target != null && s3Target.isIncludeVersions()) {
            Iterator<EcsS3ObjectVersion> sourceVersions = versionIterator(syncObject);
            Iterator<EcsS3ObjectVersion> targetVersions = s3Target.versionIterator(syncObject);

            // special workaround for bug where objects are listed, but they have no versions
            if (sourceVersions.hasNext()) {

                while (sourceVersions.hasNext()) {
                    if (!targetVersions.hasNext())
                        throw new RuntimeException("The source system has more versions of the object than the target");

                    EcsS3ObjectVersion sourceVersion = sourceVersions.next();
                    EcsS3ObjectVersion targetVersion = targetVersions.next();

                    if (sourceVersion.isLatest()) continue; // current version is verified through filter chain below

                    log.debug("#==? verifying version (source vID: {}, target vID: {})",
                            sourceVersion.getVersionId(), targetVersion.getVersionId());

                    if (sourceVersion.isDeleteMarker()) {
                        if (targetVersion.isDeleteMarker()) {
                            log.info("#==# delete marker verified for version (source vID: {}, target vID: {})",
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
                            log.info("#==# checksum verified for version (source vID: {}, target vID: {})",
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
        if (syncObject instanceof EcsS3ObjectVersion && ((EcsS3ObjectVersion) syncObject).isDeleteMarker()) {
            SyncObject targetObject = filterChain.reverseFilter(syncObject);
            try {
                targetObject.getMetadata(); // this should return a 404
                throw new RuntimeException("Latest object version is a delete marker on the source, but exists on the target");
            } catch (S3Exception e) {
                if (e.getHttpCode() != 404) throw e; // if it's not a 404, there was some other error
            }
        } else {
            super.verify(syncObject, filterChain);
        }
    }

    @Override
    public void delete(final EcsS3SyncObject syncObject) {
        time(new Function<Void>() {
            @Override
            public Void call() {
                s3.deleteObject(bucketName, syncObject.getKey());
                return null;
            }
        }, OPERATION_DELETE_OBJECT);
    }

    @Override
    public String getName() {
        return "S3 Source";
    }

    @Override
    public String getDocumentation() {
        return "Scans and reads content from an ECS S3 bucket. This " +
                "source plugin is triggered by the pattern:\n" +
                EcsS3Util.PATTERN_DESC + "\n" +
                "Scheme, host and port are all required. " +
                "root-prefix (optional) is the prefix under which to start " +
                "enumerating within the bucket, e.g. dir1/. If omitted the " +
                "root of the bucket will be enumerated.";
    }

    @Override
    public String summarizeConfig() {
        return super.summarizeConfig()
                + " - enableVHosts: " + enableVHosts + "\n"
                + " - smartClientEnabled: " + smartClientEnabled + "\n"
                + " - bucketName: " + bucketName + "\n"
                + " - rootKey: " + rootKey + "\n"
                + " - decodeKeys: " + decodeKeys + "\n"
                + " - versioningEnabled: " + versioningEnabled + "\n"
                + " - apacheClientEnabled: " + apacheClientEnabled + "\n"
                + " - sourceKeyList: " + sourceKeyList + "\n";
    }

    @Override
    public void cleanup() {
        s3.destroy();
        super.cleanup();
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

    protected class PrefixIterator extends ReadOnlyIterator<EcsS3SyncObject> {
        private String prefix;
        private ListObjectsResult listing;
        private Iterator<S3Object> objectIterator;

        public PrefixIterator(String prefix) {
            this.prefix = prefix;
        }

        @Override
        protected EcsS3SyncObject getNextObject() {
            if (listing == null || (!objectIterator.hasNext() && listing.isTruncated())) {
                getNextBatch();
            }

            if (objectIterator.hasNext()) {
                S3Object object = objectIterator.next();
                return new EcsS3SyncObject(EcsS3Source.this, s3, bucketName, object.getKey(), getRelativePath(object.getKey()), object.getSize());
            }

            // list is not truncated and iterators are finished; no more objects
            return null;
        }

        private void getNextBatch() {
            if (listing == null) {
                listing = s3.listObjects(bucketName, "".equals(prefix) ? null : prefix);
            } else {
                log.info("getting next page of objects [prefix: {}, marker: {}, nextMarker: {}, encodingType: {}, maxKeys: {}]",
                        listing.getPrefix(), listing.getMarker(), listing.getNextMarker(), listing.getEncodingType(), listing.getMaxKeys());
                listing = s3.listMoreObjects(listing);
            }
            objectIterator = listing.getObjects().iterator();
        }
    }

    protected class DeletedObjectIterator extends ReadOnlyIterator<EcsS3SyncObject> {
        private String prefix;
        private ListVersionsResult versionListing;
        private Iterator<AbstractVersion> versionIterator;

        public DeletedObjectIterator(String prefix) {
            this.prefix = prefix;
        }

        @Override
        protected EcsS3SyncObject getNextObject() {
            while (true) {
                AbstractVersion version = getNextVersion();

                if (version == null) return null;

                if (version.isLatest() && version instanceof DeleteMarker)
                    return new EcsS3ObjectVersion(EcsS3Source.this, s3, bucketName, version.getKey(), version.getVersionId(),
                            version.isLatest(), true, version.getLastModified(), null, getRelativePath(version.getKey()));
            }
        }

        protected AbstractVersion getNextVersion() {
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
                versionListing = s3.listVersions(bucketName, "".equals(prefix) ? null : prefix);
            } else {
                versionListing = s3.listMoreVersions(versionListing);
            }
            versionIterator = versionListing.getVersions().iterator();
        }
    }

    protected class CombinedIterator<T> extends ReadOnlyIterator<T> {
        private List<? extends Iterator<T>> iterators;
        private int currentIterator = 0;

        public CombinedIterator(List<? extends Iterator<T>> iterators) {
            this.iterators = iterators;
        }

        @Override
        protected T getNextObject() {
            while (currentIterator < iterators.size()) {
                if (iterators.get(currentIterator).hasNext()) return iterators.get(currentIterator).next();
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

    /**
     * @return the endpoint
     */
    public URI getEndpoint() {
        return endpoint;
    }

    /**
     * @param endpoint the endpoint to set
     */
    public void setEndpoint(URI endpoint) {
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

    public boolean isGeoPinningEnabled() {
        return geoPinningEnabled;
    }

    public void setGeoPinningEnabled(boolean geoPinningEnabled) {
        this.geoPinningEnabled = geoPinningEnabled;
    }

    public boolean isVersioningEnabled() {
        return versioningEnabled;
    }

    public void setVersioningEnabled(boolean versioningEnabled) {
        this.versioningEnabled = versioningEnabled;
    }

    public boolean isApacheClientEnabled() {
        return apacheClientEnabled;
    }

    public void setApacheClientEnabled(boolean apacheClientEnabled) {
        this.apacheClientEnabled = apacheClientEnabled;
    }

    public File getSourceKeyList() {
        return sourceKeyList;
    }

    public void setSourceKeyList(File sourceKeyList) {
        this.sourceKeyList = sourceKeyList;
    }
}

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
package com.emc.ecs.sync.config.storage;

import com.emc.ecs.sync.config.*;
import com.emc.ecs.sync.config.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.emc.ecs.sync.config.storage.EcsS3Config.PATTERN_DESC;
import static com.emc.ecs.sync.config.storage.EcsS3Config.URI_PREFIX;

@XmlRootElement
@StorageConfig(uriPrefix = URI_PREFIX)
@Label("ECS S3")
@Documentation("Reads and writes content from/to an ECS S3 bucket. This " +
        "plugin is triggered by the pattern:\n" +
        PATTERN_DESC + "\n" +
        "Scheme, host and port are all required. " +
        "key-prefix (optional) is the prefix under which to start " +
        "enumerating or writing within the bucket, e.g. dir1/. If omitted the " +
        "root of the bucket will be enumerated or written to. Note that MPUs now use " +
        "a shared thread pool per plugin instance, the size of which matches the " +
        "threadCount setting in the main options - so mpuThreadCount here has no effect.")
public class EcsS3Config extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(EcsS3Config.class);

    public static final String URI_PREFIX = "ecs-s3:";
    public static final Pattern URI_PATTERN = Pattern.compile("^" + URI_PREFIX + "(?:(http|https)://)?([^:]+):([^@]+)@([^/:]+?)(:[0-9]+)?/([^/]+)(?:/(.*))?$");
    public static final String PATTERN_DESC = URI_PREFIX + "http[s]://access_key:secret_key@hosts/bucket[/key-prefix] where hosts = host[,host][,..] or vdc-name(host,..)[,vdc-name(host,..)][,..] or load-balancer[:port]";
    public static final Pattern VDC_PATTERN = Pattern.compile("(?:([^(,]+)?[(])?([^()]+)[)]?,?");

    public static final int DEFAULT_MPU_THRESHOLD_MB = 512;
    public static final int DEFAULT_MPU_PART_SIZE_MB = 128;
    /**
     * @deprecated MPUs now use a shared thread pool per plugin instance, so you cannot specify a different pool size for MPU (this property has no effect)
     */
    @Deprecated
    public static final int DEFAULT_MPU_THREAD_COUNT = 4;
    public static final int DEFAULT_CONNECT_TIMEOUT = 15000; // 15 seconds
    // disable read timeout to prevent lost update and partial data in target if ECS stalls
    public static final int DEFAULT_READ_TIMEOUT = 0;
    public static final int MIN_PART_SIZE_MB = 4;

    private Protocol protocol;
    private String[] vdcs;
    private String host;
    private int port;
    private String accessKey;
    private String secretKey;
    private String sessionToken;
    private boolean enableVHosts;
    private boolean smartClientEnabled = true;
    private boolean geoPinningEnabled;
    private String bucketName;
    private boolean createBucket;
    private String keyPrefix;
    private boolean urlEncodeKeys;
    private boolean includeVersions;
    private boolean apacheClientEnabled = true;
    private int mpuThresholdMb = DEFAULT_MPU_THRESHOLD_MB;
    private int mpuPartSizeMb = DEFAULT_MPU_PART_SIZE_MB;
    private boolean mpuEnabled;
    private boolean mpuResumeEnabled;
    private int socketConnectTimeoutMs = DEFAULT_CONNECT_TIMEOUT;
    private int socketReadTimeoutMs = DEFAULT_READ_TIMEOUT;
    private boolean preserveDirectories = true;
    private boolean remoteCopy;
    private boolean resetInvalidContentType = true;
    private boolean storeSourceObjectCopyMarkers;

    @UriGenerator
    public String getUri(boolean scrubbed) {
        String portStr = port > 0 ? ":" + port : "";
        String pathStr = keyPrefix == null ? "" : "/" + keyPrefix;
        String hostStr = host == null ? ConfigUtil.join(vdcs) : host;
        String uriSecretKey = scrubbed ? bin(scrub(secretKey)) : bin(secretKey);
        return String.format("%s%s://%s:%s@%s%s/%s%s",
                URI_PREFIX, protocol, bin(accessKey), uriSecretKey, bin(hostStr), portStr, bin(bucketName), pathStr);
    }

    @UriParser
    public void setUri(String uri) {
        Matcher m = URI_PATTERN.matcher(uri);
        if (!m.matches()) {
            throw new ConfigurationException(String.format("URI does not match %s pattern (%s)", URI_PREFIX, PATTERN_DESC));
        }

        if (m.group(1) != null) protocol = Protocol.valueOf(m.group(1).toLowerCase());
        String hostString = m.group(4);
        if (hostString.contains(",") || hostString.contains("(")) {
            // we can't simply split on comma, because each VDC could have commas separating the hosts
            List<String> vdcList = new ArrayList<>();
            Matcher matcher = VDC_PATTERN.matcher(hostString);
            while (matcher.find()) {
                String vdc = matcher.group();
                if (vdc.charAt(vdc.length() - 1) == ',') vdc = vdc.substring(0, vdc.length() - 1);
                log.info("parsed VDC: " + vdc);
                vdcList.add(vdc);
            }
            if (!matcher.hitEnd())
                throw new ConfigurationException("invalid VDC format: " + matcher.appendTail(new StringBuffer()).toString());
            vdcs = vdcList.toArray(new String[vdcList.size()]);
        } else {
            host = hostString;
        }
        port = -1;
        if (m.group(5) != null) port = Integer.parseInt(m.group(5).substring(1));

        accessKey = m.group(2);
        secretKey = m.group(3);

        bucketName = m.group(6);
        keyPrefix = m.group(7);

        if (protocol == null || accessKey == null || secretKey == null || (host == null && (vdcs == null || vdcs.length == 0)) || bucketName == null)
            throw new ConfigurationException("protocol, accessKey, secretKey, host[s] and bucket are required");
    }

    @Option(orderIndex = 10, locations = Option.Location.Form, required = true, description = "The protocol to use when connecting to ECS (http or https)")
    public Protocol getProtocol() {
        return protocol;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    @Option(orderIndex = 20, locations = Option.Location.Form, advanced = true, description = "The VDCs to use when connecting to ECS. The format for each VDC is vdc-name(node1,node2,..). If the smart-client is enabled (default) all of the nodes in each VDC will be discovered automatically. Specify multiple entries one-per-line in the UI form")
    public String[] getVdcs() {
        return vdcs;
    }

    public void setVdcs(String[] vdcs) {
        this.vdcs = vdcs;
    }

    @Option(orderIndex = 30, locations = Option.Location.Form, description = "The load balancer or DNS name to use when connecting to ECS. Be sure to turn off the smart-client when using a load balancer")
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Option(orderIndex = 40, locations = Option.Location.Form, advanced = true, description = "Used to specify a non-standard port for a load balancer")
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Option(orderIndex = 50, locations = Option.Location.Form, required = true, description = "The ECS object user")
    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    @Option(orderIndex = 60, locations = Option.Location.Form, required = true, sensitive = true, description = "The secret key for the specified user")
    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    @Option(orderIndex = 65, sensitive = true, description = "The STS Session Token, if using temporary credentials to access ECS")
    public String getSessionToken() {
        return sessionToken;
    }

    public void setSessionToken(String sessionToken) {
        this.sessionToken = sessionToken;
    }

    @Option(orderIndex = 70, advanced = true, description = "Specifies whether virtual hosted buckets will be used (default is path-style buckets)")
    public boolean isEnableVHosts() {
        return enableVHosts;
    }

    public void setEnableVHosts(boolean enableVHosts) {
        this.enableVHosts = enableVHosts;
    }

    @Option(orderIndex = 80, cliName = "no-smart-client", cliInverted = true, advanced = true, description = "The smart-client is enabled by default. Use this option to turn it off when using a load balancer or fixed set of nodes")
    public boolean isSmartClientEnabled() {
        return smartClientEnabled;
    }

    public void setSmartClientEnabled(boolean smartClientEnabled) {
        this.smartClientEnabled = smartClientEnabled;
    }

    @Option(orderIndex = 90, advanced = true, description = "Enables geo-pinning. This will use a standard algorithm to select a consistent VDC for each object key or bucket name")
    public boolean isGeoPinningEnabled() {
        return geoPinningEnabled;
    }

    public void setGeoPinningEnabled(boolean geoPinningEnabled) {
        this.geoPinningEnabled = geoPinningEnabled;
    }

    @Option(orderIndex = 100, locations = Option.Location.Form, required = true, description = "Specifies the bucket to use")
    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 110, description = "By default, the target bucket must exist. This option will create it if it does not")
    public boolean isCreateBucket() {
        return createBucket;
    }

    public void setCreateBucket(boolean createBucket) {
        this.createBucket = createBucket;
    }

    @Option(orderIndex = 120, locations = Option.Location.Form, advanced = true, description = "The prefix to use when enumerating or writing to the bucket. Note that relative paths to objects will be relative to this prefix (when syncing to/from a different bucket or a filesystem)")
    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    @Role(RoleType.Source)
    @Option(orderIndex = 130, advanced = true, description = "Enables URL-encoding of object keys in bucket listings. Use this if a source bucket has illegal XML characters in key names")
    public boolean isUrlEncodeKeys() {
        return urlEncodeKeys;
    }

    public void setUrlEncodeKeys(boolean urlEncodeKeys) {
        this.urlEncodeKeys = urlEncodeKeys;
    }

    @Option(orderIndex = 140, advanced = true, description = "Enable to transfer all versions of every object. NOTE: this will overwrite all versions of each source key in the target system if any exist!")
    public boolean isIncludeVersions() {
        return includeVersions;
    }

    public void setIncludeVersions(boolean includeVersions) {
        this.includeVersions = includeVersions;
    }

    @Option(orderIndex = 150, advanced = true, cliInverted = true, description = "Disabling this will use the native Java HTTP protocol handler, which can be faster in some situations, but is buggy")
    public boolean isApacheClientEnabled() {
        return apacheClientEnabled;
    }

    public void setApacheClientEnabled(boolean apacheClientEnabled) {
        this.apacheClientEnabled = apacheClientEnabled;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 160, valueHint = "size-in-MB", advanced = true, description = "Sets the size threshold (in MB) when an upload shall become a multipart upload")
    public int getMpuThresholdMb() {
        return mpuThresholdMb;
    }

    public void setMpuThresholdMb(int mpuThresholdMb) {
        this.mpuThresholdMb = mpuThresholdMb;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 170, valueHint = "size-in-MB", advanced = true, description = "Sets the part size to use when multipart upload is required (objects over 5GB). Default is " + DEFAULT_MPU_PART_SIZE_MB + "MB, minimum is " + MIN_PART_SIZE_MB + "MB")
    public int getMpuPartSizeMb() {
        return mpuPartSizeMb;
    }

    public void setMpuPartSizeMb(int mpuPartSizeMb) {
        this.mpuPartSizeMb = mpuPartSizeMb;
    }

    /**
     * @deprecated MPUs now use a shared thread pool per plugin instance, so you cannot specify a different pool size for MPU (this property has no effect)
     */
    @Deprecated
    public int getMpuThreadCount() {
        return 0;
    }

    /**
     * @deprecated MPUs now use a shared thread pool per plugin instance, so you cannot specify a different pool size for MPU (this property has no effect)
     */
    @Deprecated
    public void setMpuThreadCount(int ignored) {
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 190, advanced = true, description = "Enables multi-part upload (MPU). Large files will be split into multiple streams and (if possible) sent in parallel")
    public boolean isMpuEnabled() {
        return mpuEnabled;
    }

    public void setMpuEnabled(boolean mpuEnabled) {
        this.mpuEnabled = mpuEnabled;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 195, advanced = true, description = "Enables multi-part upload (MPU) to be resumed from existing uploaded parts. ")
    public boolean isMpuResumeEnabled() {
        return mpuResumeEnabled;
    }

    public void setMpuResumeEnabled(boolean mpuResumeEnabled) {
        this.mpuResumeEnabled = mpuResumeEnabled;
    }

    @Option(orderIndex = 200, valueHint = "timeout-ms", advanced = true, description = "Sets the connection timeout in milliseconds (default is " + DEFAULT_CONNECT_TIMEOUT + "ms)")
    public int getSocketConnectTimeoutMs() {
        return socketConnectTimeoutMs;
    }

    public void setSocketConnectTimeoutMs(int socketConnectTimeoutMs) {
        this.socketConnectTimeoutMs = socketConnectTimeoutMs;
    }

    @Option(orderIndex = 210, valueHint = "timeout-ms", advanced = true, description = "Sets the read timeout in milliseconds (default is " + DEFAULT_READ_TIMEOUT + "ms)")
    public int getSocketReadTimeoutMs() {
        return socketReadTimeoutMs;
    }

    public void setSocketReadTimeoutMs(int socketReadTimeoutMs) {
        this.socketReadTimeoutMs = socketReadTimeoutMs;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 220, cliInverted = true, advanced = true, description = "By default, directories are stored in S3 as empty objects to preserve empty dirs and metadata from the source. Turn this off to avoid copying directories. Note that if this is turned off, verification may fail for all directory objects")
    public boolean isPreserveDirectories() {
        return preserveDirectories;
    }

    public void setPreserveDirectories(boolean preserveDirectories) {
        this.preserveDirectories = preserveDirectories;
    }

    @Option(orderIndex = 230, advanced = true, description = "If enabled, a remote-copy command is issued instead of streaming the data. Can only be used when the source and target is the same system")
    public boolean isRemoteCopy() {
        return remoteCopy;
    }

    public void setRemoteCopy(boolean remoteCopy) {
        this.remoteCopy = remoteCopy;
    }

    @Option(orderIndex = 240, cliInverted = true, advanced = true, description = "By default, any invalid content-type is reset to the default (application/octet-stream). Turn this off to fail these objects (ECS does not allow invalid content-types)")
    public boolean isResetInvalidContentType() {
        return resetInvalidContentType;
    }

    public void setResetInvalidContentType(boolean resetInvalidContentType) {
        this.resetInvalidContentType = resetInvalidContentType;
    }

    @Option(orderIndex = 250, advanced = true, description = "Enable this to store source object copy markers (mtime and ETag) in target user metadata. This will mark the object such that a subsequent copy job can more accurately recognize if the source object has already been copied and skip it")
    public boolean isStoreSourceObjectCopyMarkers() {
        return storeSourceObjectCopyMarkers;
    }

    public void setStoreSourceObjectCopyMarkers(boolean storeSourceObjectCopyMarkers) {
        this.storeSourceObjectCopyMarkers = storeSourceObjectCopyMarkers;
    }
}

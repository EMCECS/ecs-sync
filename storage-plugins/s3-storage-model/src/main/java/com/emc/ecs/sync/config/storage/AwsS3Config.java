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

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.RoleType;
import com.emc.ecs.sync.config.annotation.*;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.emc.ecs.sync.config.storage.AwsS3Config.PATTERN_DESC;
import static com.emc.ecs.sync.config.storage.AwsS3Config.URI_PREFIX;

@XmlRootElement
@StorageConfig(uriPrefix = URI_PREFIX)
@Label("S3")
@Documentation("Represents storage in an Amazon S3 bucket. This " +
        "plugin is triggered by the pattern:\n" +
        PATTERN_DESC + "\n" +
        "Scheme, host and port are all optional. If omitted, " +
        "https://s3.amazonaws.com:443 is assumed. " +
        "sessionToken (optional) is required for STS session credentials. " +
        "profile (optional) will allow profile credentials provider. " +
        "useDefaultCredentialsProvider (optional) enables the AWS default " +
        "credentials provider chain. " +
        "keyPrefix (optional) is the prefix under which to start " +
        "enumerating or writing keys within the bucket, e.g. dir1/. If omitted, the " +
        "root of the bucket is assumed. Note that MPUs now use " +
        "a shared thread pool per plugin instance, the size of which matches the " +
        "threadCount setting in the main options - so mpuThreadCount here has no effect.")
public class AwsS3Config extends AbstractConfig {
    public static final String URI_PREFIX = "s3:";
    public static final Pattern URI_PATTERN = Pattern.compile("^" + URI_PREFIX + "(?:(http|https)://)?([^:]+):([^@]+)@(?:([^/:]+?)?(:[0-9]+)?)?/([^/]+)(?:/(.*))?$");
    public static final String PATTERN_DESC = URI_PREFIX + "[http[s]://]access_key:secret_key@[host[:port]]/bucket[/root-prefix]";

    public static final int DEFAULT_MPU_THRESHOLD_MB = 512;
    public static final int DEFAULT_MPU_PART_SIZE_MB = 128;
    /**
     * @deprecated MPUs now use a shared thread pool per plugin instance, so you cannot specify a different pool size for MPU (this property has no effect)
     */
    @Deprecated
    public static final int DEFAULT_MPU_THREAD_COUNT = 4;
    // disable read timeout to prevent lost update and partial data in target if AWS stalls
    public static final int DEFAULT_SOCKET_TIMEOUT = 0;
    public static final int MIN_PART_SIZE_MB = 5;
    public static final String TEST_OBJECT_PREFIX = ".ecs-sync-write-access-check-";

    private Protocol protocol;
    private String host;
    private int port = -1;
    private String region;
    private boolean useDefaultCredentialsProvider;
    private String profile;
    private String accessKey;
    private String secretKey;
    private String sessionToken;
    private boolean disableVHosts;
    private String bucketName;
    private boolean createBucket;
    private String keyPrefix;
    private boolean urlDecodeKeys = true;
    private String[] excludedKeys;
    private boolean includeVersions;
    private boolean legacySignatures;
    private int mpuThresholdMb = DEFAULT_MPU_THRESHOLD_MB;
    private int mpuPartSizeMb = DEFAULT_MPU_PART_SIZE_MB;
    private boolean mpuResumeEnabled;
    private int socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT;
    private boolean preserveDirectories = true;
    private boolean sseS3Enabled;
    private String base64TlsCertificate;
    private boolean writeTestObject = false;
    private boolean storeSourceObjectCopyMarkers;

    @UriGenerator
    public String getUri(boolean scrubbed) {
        String uri = URI_PREFIX;

        if (protocol != null) uri += protocol + "://";

        String uriSecretKey = scrubbed ? bin(scrub(secretKey)) : bin(secretKey);

        uri += String.format("%s:%s@", bin(accessKey), uriSecretKey);

        if (host != null) uri += host;
        if (port > 0) uri += ":" + port;

        uri += "/" + bin(bucketName);

        if (keyPrefix != null) uri += "/" + keyPrefix;

        return uri;
    }

    @UriParser
    public void setUri(String uri) {
        Matcher m = URI_PATTERN.matcher(uri);
        if (!m.matches()) {
            throw new ConfigurationException(String.format("URI does not match %s pattern (%s)", URI_PREFIX, PATTERN_DESC));
        }


        if (m.group(1) != null) protocol = Protocol.valueOf(m.group(1).toLowerCase());
        host = m.group(4);
        port = -1;
        if (m.group(5) != null) port = Integer.parseInt(m.group(5).substring(1));

        accessKey = m.group(2);
        secretKey = m.group(3);

        bucketName = m.group(6);
        keyPrefix = m.group(7);

        if (accessKey == null || secretKey == null || bucketName == null)
            throw new ConfigurationException("accessKey, secretKey and bucket are required");
    }

    @Option(orderIndex = 10, locations = Option.Location.Form, description = "The protocol to use when connecting to S3 (http or https)")
    public Protocol getProtocol() {
        return protocol;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    @Option(orderIndex = 20, locations = Option.Location.Form, description = "The host to use when connecting to S3")
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Option(orderIndex = 30, locations = Option.Location.Form, advanced = true, description = "The port to use when connecting to S3")
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Option(orderIndex = 33, advanced = true, description = "Overrides the AWS region that would be inferred from the endpoint")
    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }


    @Option(orderIndex = 35, advanced = true, description = "Use S3 default credentials provider. See https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html")
    public boolean getUseDefaultCredentialsProvider() {
        return useDefaultCredentialsProvider;
    }

    public void setUseDefaultCredentialsProvider(boolean useDefaultCredentialsProvider) {
        this.useDefaultCredentialsProvider = useDefaultCredentialsProvider;
    }

    @Option(orderIndex = 37, advanced = true, description = "The profile name to use when providing credentials in a configuration file (via useDefaultCredentialsProvider)")
    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    @Option(orderIndex = 40, locations = Option.Location.Form, description = "The S3 access key")
    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    @Option(orderIndex = 50, locations = Option.Location.Form, sensitive = true, description = "The secret key for the specified access key")
    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    @Option(orderIndex = 55, advanced = true, sensitive = true, description = "The session token to use with temporary credentials")
    public String getSessionToken() {
        return sessionToken;
    }

    public void setSessionToken(String sessionToken) {
        this.sessionToken = sessionToken;
    }

    @Option(orderIndex = 60, advanced = true, description = "Specifies whether virtual hosted buckets will be disabled (and path-style buckets will be used)")
    public boolean isDisableVHosts() {
        return disableVHosts;
    }

    public void setDisableVHosts(boolean disableVHosts) {
        this.disableVHosts = disableVHosts;
    }

    @Option(orderIndex = 70, locations = Option.Location.Form, required = true, description = "Specifies the bucket to use")
    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 80, description = "By default, the target bucket must exist. This option will create it if it does not")
    public boolean isCreateBucket() {
        return createBucket;
    }

    public void setCreateBucket(boolean createBucket) {
        this.createBucket = createBucket;
    }

    @Option(orderIndex = 90, locations = Option.Location.Form, advanced = true, description = "The prefix to use when enumerating or writing to the bucket. Note that relative paths to objects will be relative to this prefix (when syncing to/from a different bucket or a filesystem)")
    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    @Role(RoleType.Source)
    @Option(orderIndex = 95, cliInverted = true, advanced = true, description = "In bucket list operations, the encoding-type=url parameter is always sent (to request safe encoding of keys). By default, object keys in bucket listings are then URL-decoded. Disable this for S3-compatible systems that do not support the encoding-type parameter, so that keys are pulled verbatim out of the XML. Note: disabling this on systems that *do* support the parameter may provide corrupted key names in the bucket list.")
    public boolean isUrlDecodeKeys() {
        return urlDecodeKeys;
    }

    public void setUrlDecodeKeys(boolean urlDecodeKeys) {
        this.urlDecodeKeys = urlDecodeKeys;
    }

    @Role(RoleType.Source)
    @Option(orderIndex = 100, valueHint = "regex-pattern", description = "A list of regular expressions to search against the full object key.  If the key matches, the object will not be included in the enumeration.  Since this is a regular expression, take care to escape special characters.  For example, to exclude all .md5 checksums, the pattern would be .*\\.md5. Specify multiple entries by repeating the CLI option or XML element, or using multiple lines in the UI form")
    public String[] getExcludedKeys() {
        return excludedKeys;
    }

    public void setExcludedKeys(String[] excludedKeys) {
        this.excludedKeys = excludedKeys;
    }

    @Option(orderIndex = 110, advanced = true, description = "Transfer all versions of every object. NOTE: this will overwrite all versions of each source key in the target system if any exist!")
    public boolean isIncludeVersions() {
        return includeVersions;
    }

    public void setIncludeVersions(boolean includeVersions) {
        this.includeVersions = includeVersions;
    }

    @Option(orderIndex = 120, advanced = true, description = "Specifies whether the client will use v2 auth. Necessary for ECS < 3.0")
    public boolean isLegacySignatures() {
        return legacySignatures;
    }

    public void setLegacySignatures(boolean legacySignatures) {
        this.legacySignatures = legacySignatures;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 130, valueHint = "size-in-MB", advanced = true, description = "Sets the size threshold (in MB) when an upload shall become a multipart upload")
    public int getMpuThresholdMb() {
        return mpuThresholdMb;
    }

    public void setMpuThresholdMb(int mpuThresholdMb) {
        this.mpuThresholdMb = mpuThresholdMb;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 140, valueHint = "size-in-MB", advanced = true, description = "Sets the part size to use when multipart upload is required (objects over 5GB). Default is " + DEFAULT_MPU_PART_SIZE_MB + "MB, minimum is " + MIN_PART_SIZE_MB + "MB")
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
    @Option(orderIndex = 155, advanced = true, description = "Enables multi-part upload (MPU) to be resumed from existing uploaded parts. ")
    public boolean isMpuResumeEnabled() {
        return mpuResumeEnabled;
    }

    public void setMpuResumeEnabled(boolean mpuResumeEnabled) {
        this.mpuResumeEnabled = mpuResumeEnabled;
    }

    @Option(orderIndex = 160, valueHint = "timeout-ms", advanced = true, description = "Sets the socket timeout in milliseconds (default is " + DEFAULT_SOCKET_TIMEOUT + "ms)")
    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public void setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 170, cliInverted = true, advanced = true, description = "By default, directories are stored in S3 as empty objects to preserve empty dirs and metadata from the source. Turn this off to avoid copying directories. Note that if this is turned off, verification may fail for all directory objects")
    public boolean isPreserveDirectories() {
        return preserveDirectories;
    }

    public void setPreserveDirectories(boolean preserveDirectories) {
        this.preserveDirectories = preserveDirectories;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 180, advanced = true, label = "Server-Side Encryption (SSE-S3)", description = "Specifies whether Server-Side Encryption (SSE-S3) is enabled when writing to the target")
    public boolean isSseS3Enabled() {
        return sseS3Enabled;
    }

    public void setSseS3Enabled(boolean sseS3Enabled) {
        this.sseS3Enabled = sseS3Enabled;
    }

    @Option(orderIndex = 190, advanced = true, label = "Base64 Encoded TLS Certificate", description = "Base64 Encoded TLS Certificate of the S3 Host to be trusted")
    public String getBase64TlsCertificate() {
        return base64TlsCertificate;
    }

    public void setBase64TlsCertificate(String base64TlsCertificate) {
        this.base64TlsCertificate = base64TlsCertificate;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 200, advanced = true, description = "By default, storage plugins will avoid writing any non-user data to the target. However, that is the only way to verify write access in AWS. Enable this option to write (and then delete) a test object to the target bucket during initialization, to verify write access before starting the job. The test object name will start with " + TEST_OBJECT_PREFIX)
    public boolean isWriteTestObject() {
        return writeTestObject;
    }

    public void setWriteTestObject(boolean writeTestObject) {
        this.writeTestObject = writeTestObject;
    }

    @Option(orderIndex = 210, advanced = true, description = "Enable this to store source object copy markers (mtime and ETag) in target user metadata. This will mark the object such that a subsequent copy job can more accurately recognize if the source object has already been copied and skip it")
    public boolean isStoreSourceObjectCopyMarkers() {
        return storeSourceObjectCopyMarkers;
    }

    public void setStoreSourceObjectCopyMarkers(boolean storeSourceObjectCopyMarkers) {
        this.storeSourceObjectCopyMarkers = storeSourceObjectCopyMarkers;
    }
}

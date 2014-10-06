package com.emc.vipr.sync.target;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.S3Util;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class S3Target extends SyncTarget {
    private static final Logger l4j = Logger.getLogger(S3Target.class);

    // Invalid for metadata names
    private static final char[] HTTP_SEPARATOR_CHARS = new char[]{
            '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']',
            '?', '=', ' ', '\t'};

    public static final String BUCKET_OPTION = "target-bucket";
    public static final String BUCKET_DESC = "Required. Specifies the target bucket to use";
    public static final String BUCKET_ARG_NAME = "bucket";

    public static final String DISABLE_VHOSTS_OPTION = "target-disable-vhost";
    public static final String DISABLE_VHOSTS_DESC = "If specified, virtual hosted buckets will be disabled and path-style buckets will be used.";

    private String protocol;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String bucketName;
    private String rootKey;
    private boolean disableVHosts;

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

        if (line.hasOption(BUCKET_OPTION))
            bucketName = line.getOptionValue(BUCKET_OPTION);

        disableVHosts = line.hasOption(DISABLE_VHOSTS_OPTION);
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
    public void filter(SyncObject obj) {
        try {
            // S3 doesn't support directories.
            if (obj.isDirectory()) {
                l4j.debug("Skipping directory object in S3Target: " + obj.getRelativePath());
                return;
            }

            // some sync objects lazy-load their metadata (i.e. AtmosSyncObject)
            // since this may be a timed operation, ensure it loads outside of other timed operations
            final SyncMetadata metadata = obj.getMetadata();

            // Compute target key
            String destKey = rootKey + obj.getRelativePath();
            if (destKey.startsWith("/")) {
                destKey = destKey.substring(1);
            }

            obj.setTargetIdentifier(destKey);

            // Get target metadata.
            ObjectMetadata destMeta = null;
            try {
                destMeta = s3.getObjectMetadata(bucketName, destKey);
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404) {
                    // OK
                } else {
                    throw new RuntimeException("Failed to check target key '" +
                            destKey + "' : " + e, e);
                }
            }

            Date sourceLastModified = obj.getMetadata().getModificationTime();
            long sourceSize = obj.getMetadata().getSize();

            if (!force && destMeta != null) {
                // Check overwrite
                Date destLastModified = destMeta.getLastModified();
                long destSize = destMeta.getContentLength();

                if (destLastModified.equals(sourceLastModified) && sourceSize == destSize) {
                    l4j.info(String.format(
                            "Source and target the same.  Skipping %s",
                            obj.getRelativePath()));
                    return;
                }
                if (destLastModified.after(sourceLastModified)) {
                    l4j.info(String.format(
                            "Target newer than source.  Skipping %s",
                            obj.getRelativePath()));
                    return;

                }
            }

            // Put
            ObjectMetadata om = new ObjectMetadata();
            om.setContentLength(sourceSize);
            om.setUserMetadata(formatUserMetadata(metadata));
            om.setContentType(obj.getMetadata().getContentType());
            om.setLastModified(sourceLastModified);
            PutObjectRequest req = new PutObjectRequest(bucketName, destKey,
                    obj.getInputStream(), om);

            if (includeAcl) req.setAccessControlList(S3Util.s3AclFromSyncAcl(metadata.getAcl(), ignoreInvalidAcls));

            PutObjectResult resp = s3.putObject(req);
            l4j.debug(String.format("Wrote %s etag: %s", destKey, resp.getETag()));

        } catch (Exception e) {
            throw new RuntimeException("Failed to store object: " + e, e);
        }

    }


    private Map<String, String> formatUserMetadata(SyncMetadata metadata) {
        Map<String, String> s3meta = new HashMap<>();

        for (String key : metadata.getUserMetadata().keySet()) {
            s3meta.put(filterName(key), filterValue(metadata.getUserMetadataValue(key)));
        }

        return s3meta;
    }

    /**
     * S3 metadata names must be compatible with header naming.  Filter the names so
     * they're acceptable.
     * Per HTTP RFC:<br>
     * <pre>
     * token          = 1*<any CHAR except CTLs or separators>
     * separators     = "(" | ")" | "<" | ">" | "@"
     *                 | "," | ";" | ":" | "\" | <">
     *                 | "/" | "[" | "]" | "?" | "="
     *                 | "{" | "}" | SP | HT
     * <pre>
     *
     * @param name the header name to filter.
     * @return the metadata name filtered to be compatible with HTTP headers.
     */
    private String filterName(String name) {
        try {
            // First, filter out any non-ASCII characters.
            byte[] raw = name.getBytes("US-ASCII");
            String ascii = new String(raw, "US-ASCII");

            // Strip separator chars
            for (char sep : HTTP_SEPARATOR_CHARS) {
                ascii = ascii.replace(sep, '-');
            }

            return ascii;
        } catch (UnsupportedEncodingException e) {
            // should never happen
            throw new RuntimeException("Missing ASCII encoding", e);
        }
    }

    /**
     * S3 sends metadata as HTTP headers, unencoded.  Filter values to be compatible
     * with headers.
     */
    private String filterValue(String value) {
        try {
            // First, filter out any non-ASCII characters.
            byte[] raw = value.getBytes("US-ASCII");
            String ascii = new String(raw, "US-ASCII");

            // Make sure there's no newlines
            ascii = ascii.replace('\n', ' ');

            return ascii;
        } catch (UnsupportedEncodingException e) {
            // should never happen
            throw new RuntimeException("Missing ASCII encoding", e);
        }
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
}

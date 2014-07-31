package com.emc.atmos.sync.plugins;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.sync.util.S3Utils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class S3Destination extends DestinationPlugin implements InitializingBean {
    private static final Logger l4j = Logger.getLogger(S3Destination.class);
    
    // Invalid for metadata names
    private static final char[] HTTP_SEPARATOR_CHARS = new char[] {
        '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', 
        '?', '=', ' ', '\t'};

    public static final String ACCESS_KEY_OPTION = "s3-dest-access-key";
    public static final String ACCESS_KEY_DESC = "The Amazon S3 access key, e.g. 0PN5J17HBGZHT7JJ3X82";
    public static final String ACCESS_KEY_ARG_NAME = "access-key";

    public static final String SECRET_KEY_OPTION = "s3-dest-secret-key";
    public static final String SECRET_KEY_DESC = "The Amazon S3 secret key, e.g. uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o";
    public static final String SECRET_KEY_ARG_NAME = "secret-key";

    public static final String ROOT_KEY_OPTION = "s3-dest-root-key";
    public static final String ROOT_KEY_DESC = "The destination prefix within the bucket, e.g. dir1/.  Optional, if omitted the root of the bucket will be used.";
    public static final String ROOT_KEY_ARG_NAME = "root-key";
    
    public static final String ENDPOINT_OPTION = "s3-dest-endpoint";
    public static final String ENDPOINT_DESC = "Specifies a different endpoint. If not set, s3.amazonaws.com is assumed.";
    public static final String ENDPOINT_ARG_NAME = "endpoint";
    
    public static final String DISABLE_VHOSTS_OPTION = "s3-dest-disable-vhost";
    public static final String DISABLE_VHOSTS_DESC = "If specified, virtual hosted buckets will be disabled and path-style buckets will be used.";

    private AmazonS3 amz;
    private String bucketName;
    private String rootKey;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private boolean force;

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.hasText(accessKey, "accessKey is required");
        Assert.hasText(secretKey, "secretKey is required");
        Assert.hasText(bucketName, "bucketName is required");
        AWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
        amz = new AmazonS3Client(creds);
        if(endpoint != null) {
            amz.setEndpoint(endpoint);
        }

        if (!amz.doesBucketExist(bucketName)) {
            throw new RuntimeException("The bucket " + bucketName
                    + " does not exist.");
        }
    }

    @Override
    public void filter(SyncObject obj) {
        try {
            // some sync objects lazy-load their metadata (i.e. AtmosSyncObject)
            // since this may be a timed operation, ensure it loads outside of other timed operations
            final Map<String, Metadata> umeta = obj.getMetadata().getMetadata();
            
            // S3 does not have the concept of directories.
            if(obj.isDirectory()) {
                l4j.debug("Skipping directory in S3Destination: " + obj.getRelativePath());
                return;
            }
            
            // Compute destination key
            String destKey = rootKey + obj.getRelativePath();
            if(destKey.startsWith("/")) {
                destKey = destKey.substring(1);
            }
            
            if(endpoint == null) {
                obj.setDestURI(new URI("http", bucketName+".s3.amazonaws.com", "/" + destKey, null));
            } else {
                URI u = new URI(endpoint + "/" + destKey);
                u = new URI(u.getScheme(), bucketName + "." + u.getHost(), u.getPath());
                obj.setDestURI(u);
            }
            
            // Get destination metadata.
            ObjectMetadata destMeta = null;
            try {
                destMeta = amz.getObjectMetadata(bucketName, destKey);
            } catch(AmazonS3Exception e) {
                if(e.getStatusCode() == 404) {
                    // OK
                } else {
                    throw new RuntimeException("Failed to check destination key '" + 
                            destKey + "' : " + e, e);
                }
            }
            
            Date sourceLastModified = obj.getMetadata().getMtime();
            long sourceSize = obj.getSize();
            
            if(!force && destMeta != null) {
                // Check overwrite
                Date destLastModified = destMeta.getLastModified();
                long destSize = destMeta.getContentLength();
                
                if(destLastModified.equals(sourceLastModified) && sourceSize == destSize) {
                    l4j.info(String.format(
                            "Source and destination the same.  Skipping %s", 
                            obj.getRelativePath()));
                    return;
                }
                if(destLastModified.after(sourceLastModified)) {
                    l4j.info(String.format(
                            "Destination newer than source.  Skipping %s", 
                            obj.getRelativePath()));
                    return;
                    
                }
            }
            
            // Put
            ObjectMetadata om = new ObjectMetadata();
            om.setContentLength(sourceSize);
            om.setUserMetadata(formatUserMetadata(umeta));
            om.setContentType(obj.getMetadata().getContentType());
            om.setLastModified(sourceLastModified);
            PutObjectRequest req = new PutObjectRequest(bucketName, destKey, 
                    obj.getInputStream(), om);
                    
            PutObjectResult resp = amz.putObject(req);
            l4j.debug(String.format("Wrote %s etag: %s", destKey, resp.getETag()));
            
        } catch(Exception e) {
            throw new RuntimeException("Failed to store object: " + e, e);
        }
        
    }


    private Map<String, String> formatUserMetadata(Map<String, Metadata> umeta) {
        Map<String, String> s3meta = new HashMap<String, String>();
        
        for(String key : umeta.keySet()) {
            s3meta.put(filterName(key), filterValue(umeta.get(key).getValue()));
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
     * @param name the header name to filter.
     * @return the metadata name filtered to be compatible with HTTP headers.
     */
    private String filterName(String name) {
        try {
            // First, filter out any non-ASCII characters.
            byte[] raw = name.getBytes("US-ASCII");
            String ascii = new String(raw, "US-ASCII");
            
            // Strip separator chars
            for(char sep : HTTP_SEPARATOR_CHARS) {
                ascii = ascii.replace(sep, '-');
            }
            
            return ascii;
        } catch(UnsupportedEncodingException e) {
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
        } catch(UnsupportedEncodingException e) {
            // should never happen
            throw new RuntimeException("Missing ASCII encoding", e);
        }
    }

    @SuppressWarnings("static-access")
    @Override
    public Options getOptions() {
        Options opts = new Options();
        opts.addOption(OptionBuilder.withLongOpt(ACCESS_KEY_OPTION)
                .withDescription(ACCESS_KEY_DESC).hasArg()
                .withArgName(ACCESS_KEY_ARG_NAME).create());

        opts.addOption(OptionBuilder.withLongOpt(SECRET_KEY_OPTION)
                .withDescription(SECRET_KEY_DESC).hasArg()
                .withArgName(SECRET_KEY_ARG_NAME).create());
        
        opts.addOption(OptionBuilder.withLongOpt(ROOT_KEY_OPTION)
                .withDescription(ROOT_KEY_DESC).hasArg()
                .withArgName(ROOT_KEY_ARG_NAME).create());
        
        opts.addOption(OptionBuilder.withLongOpt(ENDPOINT_OPTION)
                .withDescription(ENDPOINT_DESC).hasArg()
                .withArgName(ENDPOINT_ARG_NAME).create());
        
        opts.addOption(OptionBuilder.withLongOpt(DISABLE_VHOSTS_OPTION)
                .withDescription(DISABLE_VHOSTS_DESC).create());

        return opts;
    }

    @Override
    public boolean parseOptions(CommandLine line) {
        String destStr = line.getOptionValue(CommonOptions.DESTINATION_OPTION);
        if(destStr == null) {
            return false;
        }
        if (destStr.startsWith("s3:")) {
            bucketName = destStr.substring(3);
            
            if (!line.hasOption(ACCESS_KEY_OPTION)) {
                throw new RuntimeException("The option --" + ACCESS_KEY_OPTION
                        + " is required.");
            }
            if (!line.hasOption(SECRET_KEY_OPTION)) {
                throw new RuntimeException("The option --" + SECRET_KEY_OPTION
                        + " is required.");
            }

            accessKey = line.getOptionValue(ACCESS_KEY_OPTION);
            secretKey = line.getOptionValue(SECRET_KEY_OPTION);

            AWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
            amz = new AmazonS3Client(creds);
            if(line.hasOption(ENDPOINT_OPTION)) {
                endpoint = line.getOptionValue(ENDPOINT_OPTION);
                amz.setEndpoint(endpoint);
            }
            
            if(line.hasOption(DISABLE_VHOSTS_OPTION)) {
                l4j.info("The use of virtual hosted buckets on the s3 source has been DISABLED.  Path style buckets will be used.");
                S3ClientOptions opts = new S3ClientOptions();
                opts.setPathStyleAccess(true);
                amz.setS3ClientOptions(opts);
            }

            if (!S3Utils.doesBucketExist(amz, bucketName)) {
                throw new RuntimeException("The bucket " + bucketName
                        + " does not exist.");
            }
            
            if (line.hasOption(ROOT_KEY_OPTION)) {
                rootKey = line.getOptionValue(ROOT_KEY_OPTION);
                if(rootKey.isEmpty() || "/".equals(rootKey)) {
                    rootKey = "";
                } else if(rootKey.startsWith("/")) {
                    rootKey = rootKey.substring(1);
                }
                if(!rootKey.isEmpty() && !rootKey.endsWith("/")) {
                    rootKey += "/";
                }
            } else {
                rootKey = "";
            }
            
            return true;
        }
        return false;
    }

    @Override
    public void validateChain(SyncPlugin first) {
    }

    @Override
    public String getName() {
        return "S3 Destination";
    }

    @Override
    public String getDocumentation() {
        return "Destination that writes content to an S3 bucket.  This "
                + "destination plugin is triggered by the pattern:\n"
                + "s3:<bucket>\n" + "e.g. s3:mybucket\n.  Note that this plugin also "
                + "accepts the --force option to force overwriting destination objects "
                + "even if they are the same or newer than the source.";
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

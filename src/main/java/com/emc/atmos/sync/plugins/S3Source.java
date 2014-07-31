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
package com.emc.atmos.sync.plugins;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.sync.util.AtmosMetadata;
import com.emc.atmos.sync.util.CountingInputStream;
import com.emc.atmos.sync.util.S3Utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.Map;

/**
 * This class implements an Amazon Simple Storage Service (S3) source for data.
 * 
 * @author cwikj
 */
public class S3Source extends MultithreadedCrawlSource implements InitializingBean {
	private static final Logger l4j = Logger.getLogger(S3Source.class);

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

	private AmazonS3 amz;
	private String bucketName;
	private String rootKey;
	private String endpoint;

    private String accessKey;

    private String secretKey;
    
    private boolean decodeKeys;

    private int bufferSize = CommonOptions.DEFAULT_BUFFER_SIZE;

	public void run() {
		running = true;
		initQueue();

		// Enqueue the first task
		S3SyncObject root = new S3SyncObject(rootKey);
		S3TaskNode rootTask = new S3TaskNode(root);
		submitCrawlTask(rootTask);
		
		runQueue();
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SourcePlugin#terminate()
	 */
	@Override
	public void terminate() {
		running = false;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
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
		
		opts.addOption(OptionBuilder.withLongOpt(DECODE_KEYS_OPTION)
		        .withDescription(DECODE_KEYS_DESC).create());
		
		opts.addOption(OptionBuilder.withLongOpt(DISABLE_VHOSTS_OPTION)
		        .withDescription(DISABLE_VHOSTS_DESC).create());

		return opts;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons.cli.CommandLine)
	 */
	@Override
	public boolean parseOptions(CommandLine line) {
		String sourceStr = line.getOptionValue(CommonOptions.SOURCE_OPTION);
		if(sourceStr == null) {
			return false;
		}
		if (sourceStr.startsWith("s3:")) {
			bucketName = sourceStr.substring(3);

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
			
			if(line.hasOption(DECODE_KEYS_OPTION)) {
			    decodeKeys = true;
			}

            if (line.hasOption(CommonOptions.IO_BUFFER_SIZE_OPTION)) {
                bufferSize = Integer.parseInt(line.getOptionValue(CommonOptions.IO_BUFFER_SIZE_OPTION));
            }

			// Parse threading options
			super.parseOptions(line);

			return true;
		}

		return false;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#validateChain(com.emc.atmos.sync.plugins.SyncPlugin)
	 */
	@Override
	public void validateChain(SyncPlugin first) {
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getName()
	 */
	@Override
	public String getName() {
		return "S3 Source";
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
	 */
	@Override
	public String getDocumentation() {
		return "Scans and reads content from an Amazon S3 bucket.  This "
				+ "source plugin is triggered by the pattern:\n"
				+ "s3:<bucket>\n" + "e.g. s3:mybucket\n"
				+ "This source plugin is multithreaded and you should use the "
				+ "--source-threads option to specify how many threads to use."
				+ " The default thread count is one.";
	}
	
	class S3SyncObject extends SyncObject {
		private String key;
		private String relativePath;
		private ObjectMetadata obj;
		private CountingInputStream in;

		public S3SyncObject(String key) {
			if("".equals(key) || key.endsWith("/")) {
				setDirectory(true);
			} else {
				// object
				setDirectory(false);
				
			}
			if(key.startsWith(rootKey)) {
				relativePath = key.substring(rootKey.length());
			} else {
				relativePath = key;
			}
			
			if(decodeKeys) {
			    try {
                    new URLDecoder();
                    relativePath = URLDecoder.decode(relativePath, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    // Should never happen
                    throw new RuntimeException("Error decoding path: "+ e, e);
                }
			}
			
			this.key = key;
			
			try {
			    if(endpoint == null) {
			        setSourceURI(new URI("http", "s3.amazonaws.com", "/" + bucketName + "/" + key, null));
			    } else {
			        URI u = new URI(endpoint + "/" + key);
			        u = new URI(u.getScheme(), bucketName + "." + u.getHost(), u.getPath());
			        setSourceURI(u);
			    }
			} catch (URISyntaxException e) {
				throw new RuntimeException("Could not build URI for key " + key + ": " + e.getMessage(), e);
			}
		}
		
		public S3SyncObject(S3ObjectSummary s3os) {
			this(s3os.getKey());
			
			setSize(s3os.getSize());
		}

		@Override
		public InputStream getInputStream() {
			if(isDirectory()) {
				return null;
			}
			if(obj == null) {
				getObject();
			}
			if(in == null) {
				S3Object s3o = amz.getObject(bucketName, key);
				in = new CountingInputStream(new BufferedInputStream(s3o.getObjectContent(), bufferSize));
			}

			return in;
		}
		
		@Override
		public AtmosMetadata getMetadata() {
			if(obj == null) {
				getObject();
			}
			return super.getMetadata();
		}


		private synchronized void getObject() {
			if(isDirectory() && "".equals(key)) {
				// Root directory objects don't exist in S3, so emulate them.
				obj = new ObjectMetadata();
				return;
			}
			
			try {
				obj = amz.getObjectMetadata(bucketName, key);
			} catch(AmazonServiceException e) {
				if(isDirectory()) {
					// Sometimes directories don't exist as an object
					obj = new ObjectMetadata();
					return;
				} else {
					throw e;
				}
			}
			
			AtmosMetadata meta = getMetadata();
			
			meta.setContentType(obj.getContentType());
			meta.setMtime(obj.getLastModified());
			
			Map<String,String> umeta = obj.getUserMetadata();
			for(String mkey : umeta.keySet()) {
				meta.getMetadata().put(mkey,
						new Metadata(mkey, umeta.get(mkey), false));
			}
			
			meta.getSystemMetadata().put("size",
					new Metadata("size", ""+obj.getContentLength(), false));
		}

		@Override
		public String getRelativePath() {
			return relativePath;
		}

		@Override
		public long getBytesRead() {
			if(in == null) {
				return 0;
			}
			return in.getBytesRead();
		}
		
		public String getKey() {
			return key;
		}
		
		// With S3, when you get the S3 object, you need to ensure you close
		// the input stream.
		public void close() {
			try {
				if(in != null) {
					in.close();
				}
			} catch(Exception e) {
				// Ignore.
			}
		}
		
		@Override
		public String toString() {
		    return "S3SyncObject: " + key;
		}
		
	}
	
	class S3TaskNode implements Runnable {
		private S3SyncObject obj;

		public S3TaskNode(S3SyncObject obj) {
			this.obj = obj;
		}

		@Override
		public void run() {
			try {
				getNext().filter(obj);
				complete(obj);
			} catch(Throwable t) {
				failed(obj, t);
				return;
			} finally {
				// Need to ensure we close any S3 streams.
				obj.close();
			}
			
			if(obj.isDirectory()) {
				// Enumerate the keys
				ListObjectsRequest req = new ListObjectsRequest(bucketName, 
						obj.key, null, "/", 1000);
				ObjectListing listing;
				do {
					listing = amz.listObjects(req);
					
					// Enqueue the files.
					for(S3ObjectSummary s3os : listing.getObjectSummaries()) {
						if(s3os.getKey().equals(obj.key)) {
							// Skip the key we're listing
							continue;
						}
						
						S3SyncObject s3so = new S3SyncObject(s3os);
						S3TaskNode s3tn = new S3TaskNode(s3so);
						submitTransferTask(s3tn);
					}
					
					// Enqueue the subdirectories
					for(String subkey : listing.getCommonPrefixes()) {
						if(subkey.equals(obj.key)) {
							// Skip the key we're listing.
							continue;
						}
						l4j.debug("Adding task for subkey: " + subkey);
						S3SyncObject s3so = new S3SyncObject(subkey);
						S3TaskNode s3tn = new S3TaskNode(s3so);
						submitCrawlTask(s3tn);
					}
					
					req.setMarker(listing.getNextMarker());
				} while(listing.isTruncated());
			}
		}
		
		
		@Override
		public String toString() {
		    return "S3TaskNode for " + obj;
		}
		
	}

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

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}

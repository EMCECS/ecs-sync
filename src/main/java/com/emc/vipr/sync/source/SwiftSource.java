package com.emc.vipr.sync.source;

import com.amazonaws.services.s3.AmazonS3;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.object.S3SyncObject;
import com.emc.vipr.sync.model.object.SwiftSyncObject;
import com.emc.vipr.sync.target.S3Target;
import com.emc.vipr.sync.target.SwiftTarget;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.javaswift.joss.swift.Swift;
import org.javaswift.joss.swift.SwiftContainer;
import org.javaswift.joss.swift.SwiftStoredObject;
import org.springframework.util.Assert;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * Created by joshia7 on 6/26/15.
 */
public class SwiftSource extends SyncSource<SwiftSyncObject>{

    private static final Logger l4j = Logger.getLogger(String.valueOf(SwiftSource.class));

    public static final String CONTAINER_OPTION = "source-container";
    public static final String CONTAINER_DESC = "Required. Specifies the source bucket to use.";
    public static final String CONTAINER_ARG_NAME = "container";

    public static final String DECODE_KEYS_OPTION = "source-decode-keys";
    public static final String DECODE_KEYS_DESC = "If specified, keys will be URL-decoded after listing them.  This can fix problems if you see file or directory names with characters like %2f in them.";

    public static final String DISABLE_VHOSTS_OPTION = "source-disable-vhost";
    public static final String DISABLE_VHOSTS_DESC = "If specified, virtual hosted buckets will be disabled and path-style buckets will be used.";

    public static final String OPERATION_DELETE_OBJECT = "SwiftDeleteObject";

    private String protocol;
    private String endpoint;
    private String username;
    private String password;
    private boolean disableVHosts;
    private String containerName;
    private String rootKey;
    private boolean decodeKeys;
    private SwiftTarget swiftTarget;
    private boolean versioningEnabled = false;

    private Account account;



    @Override
    public boolean canHandleSource(String sourceUri)
        {
            return sourceUri.startsWith(SwiftUtil.URI_PREFIX);
        }




    @Override
    public Iterator<SwiftSyncObject> childIterator(SwiftSyncObject syncObject) {

        if(syncObject.isDirectory()){
            return new PrefixIterator(syncObject.getKey());
        }else{
            return  null;

        }




    }

    @Override
    public Iterator<SwiftSyncObject> iterator() {

        if(rootKey.isEmpty() || rootKey.endsWith("/")){
             if(swiftTarget!=null){

                return new PrefixIterator(rootKey);
             }
        }else{
            return Arrays.asList(new SwiftSyncObject(this, account, containerName, rootKey, getRelativePath(rootKey), false)).iterator();
        }
        return null;
    }

    @Override
    public String getName() {
        return "Swift Source";
    }

    @Override
    public String getDocumentation() {
        return "Scans and reads content from an Swift Container. This " +
                "source plugin is triggered by the pattern:\n" +
                SwiftUtil.PATTERN_DESC + "\n" +
                "Scheme, host and port are all optional. If ommitted, " +
                "https://swift.openstack.com:9024 is assumed. " +
                "root-prefix (optional) is the prefix under which to start " +
                "enumerating within the container, e.g. dir1/. If omitted the " +
                "root of the bucket will be enumerated.";
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(CONTAINER_OPTION).withDescription(CONTAINER_DESC)
                .hasArg().withArgName(CONTAINER_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(DECODE_KEYS_OPTION).withDescription(DECODE_KEYS_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(DISABLE_VHOSTS_OPTION).withDescription(DISABLE_VHOSTS_DESC).create());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {

        SwiftUtil.SwiftUri swiftUri = SwiftUtil.parseUri(sourceUri);
        protocol = swiftUri.protocol;
        endpoint = swiftUri.endpoint;
        username = swiftUri.username;
        password = swiftUri.password;
        rootKey = swiftUri.rootKey;

        if (line.hasOption(CONTAINER_OPTION))
            containerName = line.getOptionValue(CONTAINER_OPTION);

        disableVHosts = line.hasOption(DISABLE_VHOSTS_OPTION);

        decodeKeys = line.hasOption(DECODE_KEYS_OPTION);

    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        Assert.hasText(username, "username is required");
        Assert.hasText(password, "password is required");
        Assert.hasText(containerName, "containerName is required");
        Assert.isTrue(containerName.matches("[A-Za-z0-9._-]+"), containerName + " is not a valid container name");

        AccountConfig config = new AccountConfig();
        config.setUsername(username);
        config.setPassword(password);
        config.setAuthUrl(endpoint + "/v2.0/tokens");
        account = new AccountFactory(config).createAccount();

        SwiftUtil.SwiftUri swiftUri=new SwiftUtil.SwiftUri();
        swiftUri.protocol=protocol;
        swiftUri.endpoint=endpoint;
        swiftUri.username=username;
        swiftUri.password=password;



        if(!account.getContainer(containerName).exists()){
            throw new ConfigurationException("The Container " + containerName + " does not exist.");
        }

        if (rootKey == null) rootKey = ""; // make sure rootKey isn't null'

        if(target instanceof SwiftTarget){
            swiftTarget=(SwiftTarget)target;

        }


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

//Prefix Iterator part needs to done :Issue faced on analog of ObjectListing and S3ObjectSummary

       protected class PrefixIterator extends ReadOnlyIterator<SwiftSyncObject> {
        private String key;
        private SwiftContainer listing;
        private String marker;
        private Iterator<StoredObject> objectIterator;
        private Iterator<String> prefixIterator;

        public PrefixIterator(String key) {
            this.key = key;
        }

        @Override
        protected SwiftSyncObject getNextObject() {
            if(objectIterator==null || !objectIterator.hasNext()){
                getNextBatch();
            }

            /*if (listing == null || (!objectIterator.hasNext() && !prefixIterator.hasNext() && listing.isTruncated())) {
                getNextBatch();
            }*/

            if (objectIterator.hasNext()) {
                StoredObject summary = objectIterator.next();
                marker=summary.getName();
                return new SwiftSyncObject(SwiftSource.this, account, containerName, summary.getName(), getRelativePath(summary.getName()), false);
            }
            /*if (prefixIterator.hasNext()) {
                String prefix = prefixIterator.next();
                return new SwiftSyncObject(SwiftSource.this, account, containerName, prefix, getRelativePath(prefix), true);
            }*/

            // list is not truncated and iterators are finished; no more objects
            return null;
        }

        private void getNextBatch() {

            if(marker==null){
                objectIterator=account.getContainer(containerName).list(null,null,1).iterator();
            }else{
                objectIterator=account.getContainer(containerName).list(null,marker,1).iterator();
            }



           /* if (listing == null) {
                listing = account.s3.listObjects(new ListObjectsRequest(bucketName, key, null, "/", 1000));
            } else {
                listing = s3.listNextBatchOfObjects(listing);
            }
            objectIterator =listing.getAllObjects().iterator();
            prefixIterator = listing.getCommonPrefixes().iterator();*/
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


    @Override
    public void delete(final S3SyncObject syncObject) {
        if (!syncObject.isDirectory()) {
            time(new Function<Void>() {
                @Override
                public Void call() {
                    account.getContainer(containerName).getObject(syncObject.getKey()).delete();

                    return null;
                }
            }, OPERATION_DELETE_OBJECT);
        }
    }

    /**
     * @return the containerName
     */
    public String getContainerName() {
        return containerName;
    }

    /**
     * @param containerName the containername to set
     */
    public void setContainerName(String containerName) {
        this.containerName = containerName;
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
    public String getUsername() {
        return username;
    }

    /**
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public void setSecretKey(String password) {
        this.password = password;
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

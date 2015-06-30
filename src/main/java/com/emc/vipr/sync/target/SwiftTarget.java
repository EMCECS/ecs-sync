package com.emc.vipr.sync.target;

import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.object.SwiftSyncObject;
import com.emc.vipr.sync.model.object.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.S3Util;
import com.emc.vipr.sync.util.SwiftUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.springframework.util.Assert;

import java.util.*;


/**
 * Created by conerj on 6/25/15.
 */
public class SwiftTarget extends SyncTarget {

/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
    private static final Logger l4j = Logger.getLogger(SwiftTarget.class);

    public static final String CONTAINER_OPTION = "target-container";
    public static final String CONTAINER_DESC = "Required. Specifies the target container to use";
    public static final String CONTAINER_ARG_NAME = "bucket";

    public static final String CREATE_CONTAINER_OPTION = "target-create-container";
    public static final String CREATE_CONTAINER_DESC = "By default, the target container must exist. This option will create it if it does not";


    private String protocol = "http";
    private String port = "9024";

    private String endpoint;
    private String userName; //was accessKey in S3
    private String password;
    private String tenantId;
    private String containerName;
    private String rootKey;
    private boolean createBucket;
    private Account swiftAccount;


    @Override
    public boolean canHandleTarget(String targetUri) {
        return targetUri.startsWith(SwiftUtil.URI_PREFIX);
    }

    @Override
    public Options getCustomOptions() {
        
        Options opts = new Options();

        opts.addOption(new OptionBuilder().withLongOpt(CONTAINER_OPTION).withDescription(CONTAINER_DESC)
                .hasArg().withArgName(CONTAINER_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(CREATE_CONTAINER_OPTION).withDescription(CREATE_CONTAINER_DESC).create());

        return opts;
       
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        LogMF.warn(l4j, "targetUri: {0}", targetUri);
        SwiftUtil.SwiftUri swiftUri = SwiftUtil.parseUri(targetUri);
        protocol = swiftUri.protocol;
        endpoint = swiftUri.endpoint;
        userName = swiftUri.username;
        password = swiftUri.password;
        rootKey = swiftUri.rootKey;

        if (line.hasOption(CONTAINER_OPTION)) containerName = line.getOptionValue(CONTAINER_OPTION);

        createBucket = line.hasOption(CREATE_CONTAINER_OPTION);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        //check the command line options.. username, tenantId, url, containerName
        //instantiate the client
        Assert.hasText(userName, "userName is required");
        Assert.hasText(containerName, "containerName is required");
        //TODO JMC container name constraints
        //Assert.isTrue(containerName.matches("[A-Za-z0-9._-]+"), containerName + " is not a valid bucket name");

        AccountConfig config = new AccountConfig();
        config.setUsername(userName);
        config.setPassword(password);
        config.setAuthUrl(endpoint + SwiftUtil.KEYSTONE_URI);

        swiftAccount = new AccountFactory(config).createAccount();

        //create a container if it doesn't exist
        Container container = swiftAccount.getContainer(containerName);

        if (container.exists() == false && createBucket) {
            LogMF.info(l4j, "Creating container {}", containerName);
            container.create();
        }
    }

    //This SyncObject is coming from Source
    @Override
    public void filter(SyncObject obj) {
        Container destContainer;
        StoredObject destObjHandle;
        try {
            // Swift doesn't support directories.
            if (obj.isDirectory()) {
                l4j.debug("Skipping directory object in S3Target: " + obj.getRelativePath());
                return;
            }

            // Compute target key
            String targetKey = getTargetKey(obj);
            LogMF.debug(l4j, "Target Key: {0}", targetKey);
            obj.setTargetIdentifier(SwiftUtil.fullPath(containerName, targetKey));

            SwiftUtil.SwiftUri swiftUri=new SwiftUtil.SwiftUri();
            obj.setTargetIdentifier(swiftUri.toUri());

            //TODO will still need to use full path. There will be a SwiftUtil for this
            destContainer = this.swiftAccount.getContainer(this.containerName);
            destObjHandle = destContainer.getObject(targetKey);

            // normal sync (no versions)
            Date sourceLastModified = obj.getMetadata().getModificationTime();
            long sourceSize = obj.getMetadata().getSize();


            if (!force && destObjHandle.exists()) {

                // Check overwrite
                Date destLastModified = destObjHandle.getLastModifiedAsDate();
                long destSize = destObjHandle.getContentLength();

                LogMF.debug(l4j, "Source Last Modified: {0} Dest Last Modified: {1}", sourceLastModified, destLastModified);

                if (destLastModified.equals(sourceLastModified) && sourceSize == destSize) {
                    l4j.info(String.format("Source and target the same.  Skipping %s", obj.getRelativePath()));
                    return;
                }
                if (destLastModified.after(sourceLastModified)) {
                    l4j.info(String.format("Target newer than source.  Skipping %s", obj.getRelativePath()));
                    return;
                }
            }

            /*
            // at this point we know we are going to write the object
            // Put [current object version]
            if (obj instanceof S3ObjectVersion && ((S3ObjectVersion) obj).isDeleteMarker()) {

                // object has version history, but is currently deleted
                LogMF.debug(l4j, "[{0}]: deleting object in target to replicate delete marker in source.", obj.getRelativePath());
                s3.deleteObject(buckeName, targetKey);
            } else {
                PutObjectResult resp = putObject(obj, targetKey);

                // if object has new metadata after the stream (i.e. encryption checksum), we must update S3 again
                if (obj.requiresPostStreamMetadataUpdate()) {
                    LogMF.debug(l4j, "[{0}]: updating metadata after sync as required", obj.getRelativePath());
                    CopyObjectRequest cReq = new CopyObjectRequest(containerName, targetKey, containerName, targetKey);
                    cReq.setNewObjectMetadata(S3Util.s3MetaFromSyncMeta(obj.getMetadata()));
                    s3.copyObject(cReq);
                }

                l4j.debug(String.format("Wrote %s etag: %s", targetKey, resp.getETag()));
            }
            */
            this.putObject(obj, destObjHandle);

        } catch (Exception e) {
            throw new RuntimeException("Failed to store object: " + e, e);
        }
    }

    /*
    protected void putIntermediateVersions(ListIterator<S3ObjectVersion> versions, String key) {
        while (versions.hasNext()) {
            S3ObjectVersion version = versions.next();
            try {
                if (!version.isLatest()) {
                    // source has more versions; add any non-current versions that are missing from the target
                    // (current version will be added below)
                    if (version.isDeleteMarker()) {
                        LogMF.debug(l4j, "[{0}#{1}]: deleting object in target to replicate delete marker in source.",
                                version.getRelativePath(), version.getVersionId());
                        s3.deleteObject(containerName, key);
                    } else {
                        LogMF.debug(l4j, "[{0}#{1}]: replicating historical version in target.",
                                version.getRelativePath(), version.getVersionId());
                        putObject(version, key);
                    }
                }
            } catch (RuntimeException e) {
                throw new RuntimeException(String.format("sync of historical version %s failed", version.getVersionId()), e);
            }
        }
    }
    */
    
    protected void putObject(SyncObject obj, StoredObject destObjHandle) {
        destObjHandle.uploadObject(obj.getInputStream());
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        return new SwiftSyncObject(this, this.swiftAccount, containerName, getTargetKey(obj), obj.getRelativePath(), obj.isDirectory());
    }

    private String getTargetKey(SyncObject obj) {
        //return rootKey + obj.getRelativePath();
        return obj.getRelativePath();
    }

    @Override
    public String getName() {
        return "Swift Target";
    }

    @Override
    public String getDocumentation() {
        return "Target that writes content to a Swift container.  This " +
                "plugin also " +
                "accepts the --force option to force overwriting target objects " +
                "even if they are the same or newer than the source.";
    }

    public String getBucketName() {
        return containerName;
    }

    public void setBucketName(String bucketName) {
        this.containerName = bucketName;
    }

    public String getRootKey() {
        return rootKey;
    }

    public void setRootKey(String rootKey) {
        this.rootKey = rootKey;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

}

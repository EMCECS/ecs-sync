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
package com.emc.vipr.sync.target;

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.*;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.api.bean.ServiceInformation;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.api.request.CreateObjectRequest;
import com.emc.atmos.api.request.UpdateObjectRequest;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.AtmosMetadata;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Stores objects into an Atmos system.
 *
 * @author cwikj
 */
public class AtmosTarget extends SyncTarget {
    /**
     * This pattern is used to activate this plugin.
     */
    public static final String DEST_NO_UPDATE_OPTION = "no-update";
    public static final String DEST_NO_UPDATE_DESC = "If specified, no updates will be applied to the target";

    public static final String DEST_CHECKSUM_OPT = "target-checksum";
    public static final String DEST_CHECKSUM_DESC = "If specified, the atmos wschecksum feature will be applied to uploads. Valid algorithms are SHA1, or MD5. Requires Atmos 2.1+";
    public static final String DEST_CHECKSUM_ARG_NAME = "checksum-alg";

    public static final String RETENTION_DELAY_WINDOW_OPTION = "retention-delay-window";
    public static final String RETENTION_DELAY_WINDOW_DESC = "If include-retention-expiration is set, use this option to specify the Start Delay Window in the retention policy. Default is 1 second (the minimum).";
    public static final String RETENTION_DELAY_WINDOW_ARG_NAME = "seconds";

    public static final String REPLACE_META_OPTION = "target-replace-meta";
    public static final String REPLACE_META_DESC = "Atmos does not have a call to replace metadata; only to set or remove it. By default, set is used, which means removed metadata will not be reflected in existing objects. Use this flag if your sync operation might remove metadata from an existing object";

    // timed operations
    private static final String OPERATION_SET_USER_META = "AtmosSetUserMeta";
    private static final String OPERATION_SET_ACL = "AtmosSetAcl";
    private static final String OPERATION_CREATE_DIRECTORY = "AtmosCreateDirectory";
    private static final String OPERATION_CREATE_OBJECT = "AtmosCreateObject";
    private static final String OPERATION_UPDATE_OBJECT_FROM_SEGMENT = "AtmosUpdateObjectFromSegment";
    private static final String OPERATION_CREATE_OBJECT_FROM_STREAM = "AtmosCreateObjectFromStream";
    private static final String OPERATION_UPDATE_OBJECT_FROM_STREAM = "AtmosUpdateObjectFromStream";
    private static final String OPERATION_DELETE_OBJECT = "AtmosDeleteObject";
    private static final String OPERATION_SET_RETENTION_EXPIRATION = "AtmosSetRetentionExpiration";
    private static final String OPERATION_GET_SYSTEM_META = "AtmosGetSystemMeta";
    private static final String OPERATION_GET_USER_META_NAMES = "AtmosGetUserMetaNames";
    private static final String OPERATION_DELETE_USER_META = "AtmosDeleteUserMeta";
    private static final String OPERATION_TOTAL = "TotalTime";

    private static final Logger l4j = Logger.getLogger(AtmosTarget.class);

    private List<URI> endpoints;
    private String uid;
    private String secret;
    private AtmosApi atmos;
    private String destNamespace;
    private boolean noUpdate;
    private long retentionDelayWindow = 1; // 1 second by default
    private String checksum;
    private boolean replaceMeta;

    @Override
    public boolean canHandleTarget(String targetUri) {
        return targetUri.startsWith(AtmosUtil.URI_PREFIX);
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(DEST_NO_UPDATE_OPTION).withDescription(DEST_NO_UPDATE_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(DEST_CHECKSUM_OPT).withDescription(DEST_CHECKSUM_DESC)
                .hasArg().withArgName(DEST_CHECKSUM_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(RETENTION_DELAY_WINDOW_OPTION).withDescription(RETENTION_DELAY_WINDOW_DESC)
                .hasArg().withArgName(RETENTION_DELAY_WINDOW_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(REPLACE_META_OPTION).withDescription(REPLACE_META_DESC).create());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        AtmosUtil.AtmosUri atmosUri = AtmosUtil.parseUri(targetUri);
        endpoints = atmosUri.endpoints;
        uid = atmosUri.uid;
        secret = atmosUri.secret;
        destNamespace = atmosUri.rootPath;

        noUpdate = line.hasOption(DEST_NO_UPDATE_OPTION);

        checksum = line.getOptionValue(DEST_CHECKSUM_OPT);

        if (line.hasOption(RETENTION_DELAY_WINDOW_OPTION))
            retentionDelayWindow = Long.parseLong(line.getOptionValue(RETENTION_DELAY_WINDOW_OPTION));

        replaceMeta = line.hasOption(REPLACE_META_OPTION);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (atmos == null) {
            if (endpoints == null || uid == null || secret == null)
                throw new ConfigurationException("Must specify endpoints, uid and secret key");
            atmos = new AtmosApiClient(new AtmosConfig(uid, secret, endpoints.toArray(new URI[endpoints.size()])));
        }

        // Check authentication
        ServiceInformation info = atmos.getServiceInformation();
        LogMF.info(l4j, "Connected to Atmos {0} on {1}", info.getAtmosVersion(), endpoints);

        if (noUpdate)
            l4j.info("Overwrite/update target objects disabled");

        if (includeRetentionExpiration)
            l4j.info("Retention start delay window set to " + retentionDelayWindow);
    }

    @Override
    public void filter(final SyncObject<?> obj) {
        // skip the root namespace since it obviously exists
        if ("/".equals(destNamespace + obj.getRelativePath())) {
            l4j.debug("Target namespace is root");
            return;
        }

        timeOperationStart(OPERATION_TOTAL);
        try {
            // some sync objects lazy-load their metadata (i.e. AtmosSyncObject)
            // since this may be a timed operation, ensure it loads outside of other timed operations
            final Map<String, Metadata> atmosMeta = AtmosUtil.getAtmosUserMetadata(obj.getMetadata());

            ObjectIdentifier targetId = null;

            if (destNamespace != null) {
                if (obj.getTargetIdentifier() != null) {
                    targetId = new ObjectPath(obj.getTargetIdentifier());
                } else {

                    // Determine a name for the object.
                    String targetPath = destNamespace; // target namespace could be a specific file

                    // if target namespace is a directory, append the relative path
                    if (destNamespace.endsWith("/")) {
                        targetPath += obj.getRelativePath();
                        if (obj.isDirectory() && !targetPath.endsWith("/")) targetPath += "/";
                    }

                    obj.setTargetIdentifier(targetPath);
                    targetId = new ObjectPath(targetPath);
                }
            } else { // object space
                if (obj.getTargetIdentifier() != null) targetId = new ObjectId(obj.getTargetIdentifier());
            }

            Map<String, Metadata> targetSystemMeta = (targetId == null) ? null : getSystemMetadata(targetId);

            if (targetSystemMeta == null) { // CREATE

                if (obj.isDirectory()) { // CREATE DIRECTORY

                    // don't create directories in object space
                    if (!(targetId instanceof ObjectPath)) {
                        LogMF.debug(l4j, "Source {0} is a directory, but target is in objectspace, ignoring",
                                obj.getSourceIdentifier());
                    } else {

                        final ObjectIdentifier fTargetId = targetId;
                        time(new Function<ObjectId>() {
                            @Override
                            public ObjectId call() {
                                return atmos.createDirectory(((ObjectPath) fTargetId), getAtmosAcl(obj.getMetadata()),
                                        atmosMeta.values().toArray(new Metadata[atmosMeta.size()]));
                            }
                        }, OPERATION_CREATE_DIRECTORY);
                    }

                } else { // CREATE FILE

                    ObjectId targetOid;
                    if (checksum != null) {
                        targetOid = createChecksummedObject(targetId, obj);
                    } else {
                        InputStream in = obj.getInputStream();
                        try {
                            final CreateObjectRequest request = new CreateObjectRequest();
                            request.identifier(targetId).acl(getAtmosAcl(obj.getMetadata())).content(in);
                            request.setUserMetadata(atmosMeta.values());
                            request.contentLength(obj.getMetadata().getSize()).contentType(obj.getMetadata().getContentType());
                            targetOid = time(new Function<ObjectId>() {
                                @Override
                                public ObjectId call() {
                                    return atmos.createObject(request).getObjectId();
                                }
                            }, OPERATION_CREATE_OBJECT_FROM_STREAM);
                        } finally {
                            safeClose(in);
                        }
                    }

                    // set our target id if we're in object space
                    if (targetId == null) {
                        targetId = targetOid;
                        obj.setTargetIdentifier(targetOid.toString());
                    }

                    if (obj.requiresPostStreamMetadataUpdate()) updateUserMeta(targetId, obj);

                    if (includeRetentionExpiration) updateRetentionExpiration(targetId, obj);
                }

            } else { // UPDATE?

                if (noUpdate) {
                    LogMF.debug(l4j, "Skipping {0}, updates disabled.", obj.getTargetIdentifier());
                } else {

                    if (obj.isDirectory()) { // UPDATE DIRECTORY?

                        if (metadataChanged(obj, targetSystemMeta) || force) {
                            updateUserMeta(targetId, obj);
                            updateAcl(targetId, obj);
                        } else {
                            LogMF.debug(l4j, "No changes from source {0} to dest {1}",
                                    obj.getSourceIdentifier(), obj.getTargetIdentifier());
                        }

                    } else { // UPDATE FILE?

                        if (dataChanged(obj, targetSystemMeta) || force) {
                            if (checksum != null) {
                                // you cannot update a checksummed object; delete and replace.
                                if (targetId instanceof ObjectId)
                                    throw new RuntimeException("Cannot update checksummed object by ObjectID, only namespace objects are supported");

                                final ObjectIdentifier fTargetId = targetId;
                                time(new Function<Void>() {
                                    @Override
                                    public Void call() {
                                        atmos.delete(fTargetId);
                                        return null;
                                    }
                                }, OPERATION_DELETE_OBJECT);
                                createChecksummedObject(targetId, obj);

                            } else {
                                // delete existing metadata if necessary
                                if (replaceMeta) deleteUserMeta(targetId);

                                InputStream in = obj.getInputStream();
                                try {
                                    final UpdateObjectRequest request = new UpdateObjectRequest();
                                    request.identifier(targetId).acl(getAtmosAcl(obj.getMetadata())).content(in);
                                    request.setUserMetadata(atmosMeta.values());
                                    request.contentLength(obj.getMetadata().getSize()).contentType(obj.getMetadata().getContentType());
                                    time(new Function<Void>() {
                                        @Override
                                        public Void call() {
                                            atmos.updateObject(request);
                                            return null;
                                        }
                                    }, OPERATION_UPDATE_OBJECT_FROM_STREAM);
                                } finally {
                                    safeClose(in);
                                }
                            }

                            if (obj.requiresPostStreamMetadataUpdate()) updateUserMeta(targetId, obj);

                            if (includeRetentionExpiration) updateRetentionExpiration(targetId, obj);

                        } else if (metadataChanged(obj, targetSystemMeta)) {
                            updateUserMeta(targetId, obj);
                            updateAcl(targetId, obj);
                            if (includeRetentionExpiration) updateRetentionExpiration(targetId, obj);

                        } else {
                            LogMF.debug(l4j, "No changes from source {0} to dest {1}",
                                    obj.getSourceIdentifier(), obj.getTargetIdentifier());
                        }
                    }
                }
            }

            LogMF.debug(l4j, "Wrote source {0} to dest {1}", obj.getSourceIdentifier(), obj.getTargetIdentifier());

            timeOperationComplete(OPERATION_TOTAL);
        } catch (Exception e) {
            timeOperationFailed(OPERATION_TOTAL);
            throw new RuntimeException(
                    "Failed to store object: " + e.getMessage(), e);
        }
    }

    @Override
    public String getName() {
        return "Atmos Target";
    }

    @Override
    public String getDocumentation() {
        return "The Atmos target plugin is triggered by the target pattern:\n" +
                AtmosUtil.PATTERN_DESC + "\n" +
                "Note that the uid should be the 'full token ID' including the " +
                "subtenant ID and the uid concatenated by a slash\n" +
                "If you want to software load balance across multiple hosts, " +
                "you can provide a comma-delimited list of hostnames or IPs " +
                "in the host part of the URI.\n" +
                "By default, objects will be written to Atmos using the " +
                "object API unless namespace-path is specified.\n" +
                "When namespace-path is used, the --force flag may be used " +
                "to overwrite target objects even if they exist.";
    }

    private Acl getAtmosAcl(SyncMetadata metadata) {
        if (!includeAcl || metadata == null || metadata.getAcl() == null) return null;

        return AtmosMetadata.atmosAclfromSyncAcl(metadata.getAcl(), ignoreInvalidAcls);
    }

    private boolean dataChanged(SyncObject obj, Map<String, Metadata> targetSystemMeta) {
        Date srcMtime = obj.getMetadata().getModificationTime();
        Date dstMtime = parseDate(targetSystemMeta.get("mtime"));

        return srcMtime != null && dstMtime != null && srcMtime.after(dstMtime);
    }

    private boolean metadataChanged(SyncObject obj, Map<String, Metadata> targetSystemMeta) {
        Date srcCtime = obj.getMetadata().getModificationTime(); // use mtime by default
        if (obj.getMetadata() instanceof AtmosMetadata) {
            srcCtime = parseDate(((AtmosMetadata) obj.getMetadata()).getSystemMetadataValue("ctime"));
        }
        Date dstCtime = parseDate(targetSystemMeta.get("ctime"));

        return srcCtime != null && dstCtime != null && srcCtime.after(dstCtime);
    }

    private ObjectId createChecksummedObject(ObjectIdentifier targetId, SyncObject obj)
            throws NoSuchAlgorithmException, IOException {
        Map<String, Metadata> atmosMeta = AtmosUtil.getAtmosUserMetadata(obj.getMetadata());
        RunningChecksum ck = new RunningChecksum(ChecksumAlgorithm.valueOf(checksum));
        byte[] buffer = new byte[1024 * 1024];
        long read = 0;
        int c;
        ObjectId targetOid;

        // create
        final CreateObjectRequest cRequest = new CreateObjectRequest();
        cRequest.identifier(targetId).acl(getAtmosAcl(obj.getMetadata()));
        cRequest.setUserMetadata(atmosMeta.values());
        cRequest.contentType(obj.getMetadata().getContentType()).wsChecksum(ck);
        targetOid = time(new Function<ObjectId>() {
            @Override
            public ObjectId call() {
                return atmos.createObject(cRequest).getObjectId();
            }
        }, OPERATION_CREATE_OBJECT);

        InputStream in = obj.getInputStream();
        try {
            while ((c = in.read(buffer)) != -1) {
                // append
                ck.update(buffer, 0, c);
                final UpdateObjectRequest uRequest = new UpdateObjectRequest();
                uRequest.identifier(targetId).content(new BufferSegment(buffer, 0, c));
                uRequest.range(new Range(read, read + c - 1)).wsChecksum(ck);
                uRequest.contentType(obj.getMetadata().getContentType());
                time(new Function<Object>() {
                    @Override
                    public Object call() {
                        atmos.updateObject(uRequest);
                        return null;
                    }
                }, OPERATION_UPDATE_OBJECT_FROM_SEGMENT);
                read += c;
            }
        } finally {
            safeClose(in);
        }

        return targetOid;
    }

    private void updateUserMeta(final ObjectIdentifier targetId, final SyncObject obj) {
        if (replaceMeta) deleteUserMeta(targetId);
        final Map<String, Metadata> atmosMeta = AtmosUtil.getAtmosUserMetadata(obj.getMetadata());
        if (atmosMeta != null && atmosMeta.size() > 0) {
            LogMF.debug(l4j, "Updating metadata on {0}", targetId);
            time(new Function<Void>() {
                @Override
                public Void call() {
                    atmos.setUserMetadata(targetId, atmosMeta.values().toArray(new Metadata[atmosMeta.size()]));
                    return null;
                }
            }, OPERATION_SET_USER_META);
        }
    }

    private void deleteUserMeta(final ObjectIdentifier targetId) {
        final Set<String> metaNames = time(new Function<Set<String>>() {
            @Override
            public Set<String> call() {
                return atmos.getUserMetadataNames(targetId).keySet();
            }
        }, OPERATION_GET_USER_META_NAMES);
        if (!metaNames.isEmpty()) {
            time(new Function<Void>() {
                @Override
                public Void call() {
                    atmos.deleteUserMetadata(targetId, metaNames.toArray(new String[metaNames.size()]));
                    return null;
                }
            }, OPERATION_DELETE_USER_META);
        }
    }

    private void updateAcl(final ObjectIdentifier targetId, final SyncObject obj) {
        final Acl atmosAcl = getAtmosAcl(obj.getMetadata());
        if (atmosAcl != null) {
            LogMF.debug(l4j, "Updating ACL on {0}", targetId);
            time(new Function<Void>() {
                @Override
                public Void call() {
                    atmos.setAcl(targetId, atmosAcl);
                    return null;
                }
            }, OPERATION_SET_ACL);
        }
    }

    private void updateRetentionExpiration(final ObjectIdentifier destId, final SyncObject obj) {
        try {
            final List<Metadata> retExpList = AtmosUtil.getExpirationMetadataForUpdate(obj);
            retExpList.addAll(AtmosUtil.getRetentionMetadataForUpdate(obj));
            if (retExpList.size() > 0) {
                time(new Function<Void>() {
                    @Override
                    public Void call() {
                        atmos.setUserMetadata(destId, retExpList.toArray(new Metadata[retExpList.size()]));
                        return null;
                    }
                }, OPERATION_SET_RETENTION_EXPIRATION);
            }
        } catch (AtmosException e) {
            LogMF.error(l4j, "Failed to manually set retention/expiration\n" +
                    "(destId: {0}, retentionEnd: {1}, expiration: {2})\n" +
                    "[http: {3}, atmos: {4}, msg: {5}]", new Object[]{
                    destId, Iso8601Util.format(AtmosUtil.getRetentionEndDate(obj.getMetadata())),
                    Iso8601Util.format(obj.getMetadata().getExpirationDate()),
                    e.getHttpCode(), e.getErrorCode(), e.getMessage()});
        } catch (RuntimeException e) {
            LogMF.error(l4j, "Failed to manually set retention/expiration\n" +
                    "(destId: {0}, retentionEnd: {1}, expiration: {2})\n[error: {3}]", new Object[]{
                    destId, Iso8601Util.format(AtmosUtil.getRetentionEndDate(obj.getMetadata())),
                    Iso8601Util.format(obj.getMetadata().getExpirationDate()), e.getMessage()});
        }
    }

    /**
     * Tries to parse an ISO-8601 date out of a metadata value.  If the value
     * is null or the parse fails, null is returned.
     *
     * @param m the metadata value
     * @return the Date or null if a date could not be parsed from the value.
     */
    private Date parseDate(Metadata m) {
        if (m == null || m.getValue() == null) {
            return null;
        }
        return parseDate(m.getValue());
    }

    private Date parseDate(String s) {
        return Iso8601Util.parse(s);
    }

    /**
     * Get system metadata.  IFF the object doesn't exist, return null.  On any
     * other error (e.g. permission denied), throw exception.
     */
    private Map<String, Metadata> getSystemMetadata(final ObjectIdentifier identifier) {
        try {
            return time(new Function<Map<String, Metadata>>() {
                @Override
                public Map<String, Metadata> call() {
                    return atmos.getSystemMetadata(identifier);
                }
            }, OPERATION_GET_SYSTEM_META);
        } catch (AtmosException e) {
            if (e.getErrorCode() == 1003) {
                // Object not found --OK
                return null;
            } else {
                throw e;
            }
        }
    }

    public String getDestNamespace() {
        return destNamespace;
    }

    public void setDestNamespace(String destNamespace) {
        this.destNamespace = destNamespace;
    }

    public List<URI> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(List<URI> endpoints) {
        this.endpoints = endpoints;
    }

    public String getChecksum() {
        return checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public boolean isNoUpdate() {
        return noUpdate;
    }

    public void setNoUpdate(boolean noUpdate) {
        this.noUpdate = noUpdate;
    }

    public long getRetentionDelayWindow() {
        return retentionDelayWindow;
    }

    public void setRetentionDelayWindow(long retentionDelayWindow) {
        this.retentionDelayWindow = retentionDelayWindow;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    /**
     * @return the atmos
     */
    public AtmosApi getAtmos() {
        return atmos;
    }

    /**
     * @param atmos the atmos to set
     */
    public void setAtmos(AtmosApi atmos) {
        this.atmos = atmos;
    }

    public boolean isReplaceMeta() {
        return replaceMeta;
    }

    public void setReplaceMeta(boolean replaceMeta) {
        this.replaceMeta = replaceMeta;
    }
}

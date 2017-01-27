/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.storage;

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.*;
import com.emc.atmos.api.bean.*;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.api.request.CreateObjectRequest;
import com.emc.atmos.api.request.ListDirectoryRequest;
import com.emc.atmos.api.request.UpdateObjectRequest;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.storage.AtmosConfig;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.util.*;
import com.emc.object.util.ProgressInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.emc.ecs.sync.config.storage.AtmosConfig.AccessType.namespace;
import static com.emc.ecs.sync.config.storage.AtmosConfig.AccessType.objectspace;

public class AtmosStorage extends AbstractStorage<AtmosConfig> {
    private static final Logger log = LoggerFactory.getLogger(AtmosStorage.class);

    public static final String PROP_ATMOS_METADATA = "atmos.metadata";

    private static final String TYPE_PROP = "type";
    private static final String MTIME_PROP = "mtime";
    private static final String CTIME_PROP = "ctime";
    private static final String SIZE_PROP = "size";
    private static final String UID_PROP = "uid";

    private static final String DIRECTORY_TYPE = "directory";

    // timed operations
    private static final String OPERATION_LIST_DIRECTORY = "AtmosListDirectory";
    private static final String OPERATION_GET_ALL_META = "AtmosGetAllMeta";
    private static final String OPERATION_GET_OBJECT_INFO = "AtmosGetObjectInfo";
    private static final String OPERATION_GET_USER_META = "AtmosGetUserMeta";
    private static final String OPERATION_DELETE_USER_META = "AtmosDeleteUserMeta";
    private static final String OPERATION_DELETE_OBJECT = "AtmosDeleteObject";
    private static final String OPERATION_SET_USER_META = "AtmosSetUserMeta";
    private static final String OPERATION_SET_ACL = "AtmosSetAcl";
    private static final String OPERATION_CREATE_DIRECTORY = "AtmosCreateDirectory";
    private static final String OPERATION_CREATE_OBJECT = "AtmosCreateObject";
    private static final String OPERATION_UPDATE_OBJECT_FROM_SEGMENT = "AtmosUpdateObjectFromSegment";
    private static final String OPERATION_CREATE_OBJECT_FROM_STREAM = "AtmosCreateObjectFromStream";
    private static final String OPERATION_UPDATE_OBJECT_FROM_STREAM = "AtmosUpdateObjectFromStream";
    private static final String OPERATION_READ_OBJECT_STREAM = "AtmosReadObjectStream";
    private static final String OPERATION_SET_RETENTION_EXPIRATION = "AtmosSetRetentionExpiration";
    private static final String OPERATION_GET_USER_META_NAMES = "AtmosGetUserMetaNames";

    private AtmosApi atmos;
    private ObjectSummary rootSummary;

    @Override
    public String getRelativePath(String identifier, boolean directory) {
        if (config.getAccessType() == objectspace) {
            return identifier;
        } else {
            String rootPath = rootSummary.getIdentifier();
            String relativePath = identifier;

            if (identifier.startsWith(rootPath)) relativePath = identifier.substring(rootPath.length());

            // remove leading and trailing slashes from relative path
            if (relativePath.startsWith("/")) relativePath = relativePath.substring(1);
            if (relativePath.endsWith("/")) relativePath = relativePath.substring(0, relativePath.length() - 1);

            return relativePath;
        }
    }

    @Override
    public String getIdentifier(String relativePath, boolean directory) {
        if (config.getAccessType() == objectspace) {
            return relativePath;
        } else {
            if (!rootSummary.isDirectory() && relativePath != null && relativePath.length() > 0)
                throw new RuntimeException("target path is a file, but source is a directory");

            // start with path in configuration
            String rootPath = rootSummary.getIdentifier();

            if (relativePath == null) relativePath = ""; // shouldn't happen

            // remove leading slashes from relative path (there shouldn't be any though)
            if (relativePath.startsWith("/")) relativePath = relativePath.substring(1);

            // add trailing slash for directories
            if (directory) relativePath += "/";

            // concatenate
            return rootPath + relativePath;
        }
    }

    private ObjectIdentifier getObjectIdentifier(String identifier) {
        switch (config.getAccessType()) {
            case namespace:
                return new ObjectPath(identifier);
            case objectspace:
            default:
                return new ObjectId(identifier);
        }
    }

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        if (config.getProtocol() == null || config.getHosts() == null || config.getUid() == null || config.getSecret() == null)
            throw new ConfigurationException("must specify endpoints, uid and secret key");

        List<URI> endpoints = new ArrayList<>();
        for (String host : config.getHosts()) {
            try {
                endpoints.add(new URI(config.getProtocol().toString(), null, host, config.getPort(), null, null, null));
            } catch (URISyntaxException e) {
                throw new ConfigurationException("invalid host: " + host);
            }
        }

        com.emc.atmos.api.AtmosConfig atmosConfig = new com.emc.atmos.api.AtmosConfig(
                config.getUid(), config.getSecret(), endpoints.toArray(new URI[endpoints.size()]));

        atmos = new AtmosApiClient(atmosConfig);

        // Check authentication
        ServiceInformation info = atmos.getServiceInformation();
        log.info("Connected to Atmos {} on {}", info.getAtmosVersion(), endpoints);

        if (config.getAccessType() == namespace) {
            if (config.getPath() == null) config.setPath("/");

            if (!config.getPath().startsWith("/")) config.setPath("/" + config.getPath());

            if (!config.getPath().equals("/")) {
                config.setPath(config.getPath().replaceFirst("/$", "")); // remove trailing slash

                // does path exist?
                try {
                    Map<String, Metadata> sysMeta = atmos.getSystemMetadata(new ObjectPath(config.getPath()));
                    Metadata typeMeta = sysMeta.get(TYPE_PROP);
                    if (typeMeta != null && DIRECTORY_TYPE.equals(typeMeta.getValue())) {
                        if (!config.getPath().endsWith("/")) config.setPath(config.getPath() + "/");
                        rootSummary = new ObjectSummary(config.getPath(), true, 0);
                    } else {
                        Metadata sizeMeta = sysMeta.get(SIZE_PROP);
                        long size = sizeMeta == null ? 0L : Long.parseLong(sizeMeta.getValue());
                        rootSummary = new ObjectSummary(config.getPath(), false, size);
                    }
                } catch (AtmosException e) {
                    if (e.getErrorCode() == 1003) {
                        if (source == this) { // we can create the target path, but source path must exist
                            throw new ConfigurationException("specified path does not exist in the subtenant");
                        } else {
                            // we will create a directory in the target
                            if (!config.getPath().endsWith("/")) config.setPath(config.getPath() + "/");
                            rootSummary = new ObjectSummary(config.getPath(), true, 0);
                        }
                    } else throw new ConfigurationException("could not locate path " + config.getPath(), e);
                }
            } else {
                rootSummary = new ObjectSummary("/", true, 0);
            }
        } else {
            if (options.getSourceListFile() == null || options.getSourceListFile().isEmpty())
                throw new ConfigurationException("you must provide a source list file for objectspace (Atmos cannot enumerate OIDs)");

            if (this == target && config.isPreserveObjectId()) {
                if (!(source instanceof AtmosStorage && ((AtmosStorage) source).getConfig().getAccessType() == objectspace
                        && getConfig().getAccessType() == objectspace))
                    throw new ConfigurationException("Preserving object IDs is only possible when both the source and target are Atmos using the objectspace access-type");

                // there's no way in the Atmos API to check the ECS version, so just try it and see what happens
                String objectId = "574e49dea38dc7990574e55963a6110587590b528051";
                CreateObjectResponse response = atmos.createObject(new CreateObjectRequest().customObjectId(objectId));
                atmos.delete(response.getObjectId());
                if (!objectId.equals(response.getObjectId().getId()))
                    throw new ConfigurationException("Preserving object IDs is not supported in the target system (requires ECS 3.0+)");
            }
        }
    }

    @Override
    protected ObjectSummary createSummary(String identifier) {
        com.emc.atmos.api.bean.ObjectMetadata atmosMetadata = getAtmosMetadata(getObjectIdentifier(identifier));

        boolean directory = DIRECTORY_TYPE.equals(
                atmosMetadata.getMetadata().get(TYPE_PROP).getValue());
        String sizeStr = atmosMetadata.getMetadata().get(SIZE_PROP).getValue();
        long size = sizeStr == null ? 0L : Long.parseLong(sizeStr);

        return new ObjectSummary(identifier, directory, size);
    }

    @Override
    public Iterable<ObjectSummary> allObjects() {
        if (config.getAccessType() == AtmosConfig.AccessType.objectspace)
            throw new UnsupportedOperationException("cannot enumerate objectspace");

        return children(rootSummary); // this is established inside configure(...)
    }

    @Override
    public Iterable<ObjectSummary> children(final ObjectSummary parent) {
        if (parent.isDirectory()) {
            return new Iterable<ObjectSummary>() {
                @Override
                public Iterator<ObjectSummary> iterator() {
                    return new DirectoryIterator(new ObjectPath(parent.getIdentifier()));
                }
            };
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public SyncObject loadObject(final String identifier) throws ObjectNotFoundException {
        if (identifier == null) throw new ObjectNotFoundException();

        try {
            ObjectIdentifier id = getObjectIdentifier(identifier);
            final com.emc.atmos.api.bean.ObjectMetadata atmosMeta = getAtmosMetadata(id);
            ObjectMetadata metadata = getSyncMeta(id, atmosMeta);

            Metadata uidMeta = atmosMeta.getMetadata().get(UID_PROP);
            String uid = uidMeta == null ? null : uidMeta.getValue();
            ObjectAcl acl = getSyncAcl(uid, atmosMeta.getAcl());

            LazyValue<InputStream> lazyStream = new LazyValue<InputStream>() {
                @Override
                public InputStream get() {
                    return readDataStream(identifier);
                }
            };

            SyncObject object = new SyncObject(this, getRelativePath(identifier, metadata.isDirectory()), metadata).withAcl(acl)
                    .withLazyStream(lazyStream);

            object.setProperty(PROP_ATMOS_METADATA, atmosMeta);

            return object;
        } catch (AtmosException e) {
            if (e.getHttpCode() == 404) throw new ObjectNotFoundException(identifier, e);
            throw e;
        }
    }

    private static final String[] SYSTEM_METADATA_TAGS = new String[]{
            "atime",
            "ctime",
            "gid",
            "itime",
            MTIME_PROP,
            "nlink",
            "objectid",
            "objname",
            "parent",
            "policyname",
            SIZE_PROP,
            TYPE_PROP,
            UID_PROP,
            "x-emc-wschecksum"
    };
    private static final Set<String> SYSTEM_TAGS =
            Collections.unmodifiableSet(
                    new HashSet<>(Arrays.asList(SYSTEM_METADATA_TAGS)));

    // tags that should not be returned as user metadata, but in rare cases have been
    private static final String[] BAD_USERMETA_TAGS = new String[]{
            "user.maui.expirationEnd",
            "user.maui.retentionEnd"
    };
    private static final Set<String> BAD_TAGS =
            Collections.unmodifiableSet(
                    new HashSet<>(Arrays.asList(BAD_USERMETA_TAGS)));

    private ObjectMetadata getSyncMeta(final ObjectIdentifier id, com.emc.atmos.api.bean.ObjectMetadata atmosMeta) {
        ObjectMetadata metadata = new ObjectMetadata();

        Metadata type = atmosMeta.getMetadata().get(TYPE_PROP);
        Metadata size = atmosMeta.getMetadata().get(SIZE_PROP);
        Metadata mtime = atmosMeta.getMetadata().get(MTIME_PROP);
        Metadata ctime = atmosMeta.getMetadata().get(CTIME_PROP);

        Map<String, ObjectMetadata.UserMetadata> userMeta = new HashMap<>();
        for (Metadata m : atmosMeta.getMetadata().values()) {
            if (!BAD_TAGS.contains(m.getName()) && !SYSTEM_TAGS.contains(m.getName())) {
                userMeta.put(m.getName(), new ObjectMetadata.UserMetadata(m.getName(), m.getValue(), m.isListable()));
            }
        }

        metadata.setContentType(atmosMeta.getContentType());
        metadata.setDirectory(type != null && DIRECTORY_TYPE.equals(type.getValue()));
        // correct for directory size (why does Atmos report size > 0?)
        if (size != null && !metadata.isDirectory()) metadata.setContentLength(Long.parseLong(size.getValue()));
        if (mtime != null) metadata.setModificationTime(Iso8601Util.parse(mtime.getValue()));
        if (ctime != null) metadata.setMetaChangeTime(Iso8601Util.parse(ctime.getValue()));
        metadata.setUserMetadata(userMeta);

        if (atmosMeta.getWsChecksum() != null)
            metadata.setChecksum(new Checksum(atmosMeta.getWsChecksum().getAlgorithm().toString(), atmosMeta.getWsChecksum().getValue()));

        if (options.isSyncRetentionExpiration() && !metadata.isDirectory()) {
            ObjectInfo info = time(new Function<ObjectInfo>() {
                @Override
                public ObjectInfo call() {
                    return atmos.getObjectInfo(id);
                }
            }, OPERATION_GET_OBJECT_INFO);
            if (info.getRetention() != null && info.getRetention().isEnabled()) {
                metadata.setRetentionEndDate(info.getRetention().getEndAt());
            }
            if (info.getExpiration() != null) {
                metadata.setExpirationDate(info.getExpiration().getEndAt());
            }
        }

        return metadata;
    }

    private ObjectAcl getSyncAcl(String uid, Acl acl) {
        ObjectAcl objectAcl = new ObjectAcl();
        objectAcl.setOwner(uid);
        for (String user : acl.getUserAcl().keySet()) {
            objectAcl.addUserGrant(user, acl.getUserAcl().get(user).toString());
        }
        for (String group : acl.getGroupAcl().keySet()) {
            objectAcl.addGroupGrant(group, acl.getGroupAcl().get(group).toString());
        }
        return objectAcl;
    }

    private InputStream readDataStream(final String identifier) {
        return time(new Function<InputStream>() {
            @Override
            public InputStream call() {
                return atmos.readObjectStream(getObjectIdentifier(identifier), null).getObject();
            }
        }, OPERATION_READ_OBJECT_STREAM);
    }

    private com.emc.atmos.api.bean.ObjectMetadata getAtmosMetadata(final ObjectIdentifier id) {
        return time(new Function<com.emc.atmos.api.bean.ObjectMetadata>() {
            @Override
            public com.emc.atmos.api.bean.ObjectMetadata call() {
                return atmos.getObjectMetadata(id);
            }
        }, OPERATION_GET_ALL_META);
    }

    @Override
    public String createObject(final SyncObject object) {
        final String identifier = getIdentifier(object.getRelativePath(), object.getMetadata().isDirectory());
        final Collection<Metadata> userMeta = getAtmosUserMetadata(object.getMetadata()).values();
        com.emc.atmos.api.bean.ObjectMetadata sourceAtmosMeta = (com.emc.atmos.api.bean.ObjectMetadata) object.getProperty(PROP_ATMOS_METADATA);

        // skip the root namespace since it obviously exists
        if ("/".equals(identifier) || "".equals(identifier)) {
            log.debug("Target is the root of the namespace");
            return identifier;
        }

        // create directory
        if (object.getMetadata().isDirectory()) {
            if (config.getAccessType() == objectspace) {
                log.debug("{} is a directory, but target is in objectspace, ignoring", identifier);
                return null;
            } else {
                time(new Function<Void>() {
                    @Override
                    public Void call() {
                        atmos.createDirectory(new ObjectPath(identifier),
                                options.isSyncAcl() ? getAtmosAcl(object.getAcl()) : null,
                                options.isSyncMetadata() ? userMeta.toArray(new Metadata[userMeta.size()]) : new Metadata[0]);
                        return null;
                    }
                }, OPERATION_CREATE_DIRECTORY);
                return identifier;
            }

            // create object
        } else {
            try {
                ObjectIdentifier targetId = null;
                if (config.getAccessType() == namespace) targetId = new ObjectPath(identifier);

                ObjectId targetOid;
                if (config.getWsChecksumType() != null) {
                    targetOid = createChecksummedObject(targetId, object, config.getWsChecksumType());
                } else if (sourceAtmosMeta != null && sourceAtmosMeta.getWsChecksum() != null) {
                    AtmosConfig.Hash checksumType = AtmosConfig.Hash.valueOf(sourceAtmosMeta.getWsChecksum().getAlgorithm().toString().toLowerCase());
                    targetOid = createChecksummedObject(targetId, object, checksumType);
                } else {
                    try (InputStream in = options.isSyncData() ? object.getDataStream() : new ByteArrayInputStream(new byte[0])) {
                        final CreateObjectRequest request = new CreateObjectRequest();

                        if (options.isMonitorPerformance())
                            request.setContent(new ProgressInputStream(in, new PerformanceListener(getWriteWindow())));
                        else request.setContent(in);

                        request.identifier(targetId);
                        if (options.isSyncAcl()) request.acl(getAtmosAcl(object.getAcl()));
                        if (options.isSyncMetadata())
                            request.contentType(object.getMetadata().getContentType()).setUserMetadata(userMeta);
                        request.setContentLength(object.getMetadata().getContentLength());
                        // preserve object ID
                        if (config.isPreserveObjectId()) request.setCustomObjectId(object.getRelativePath());
                        targetOid = time(new Function<ObjectId>() {
                            @Override
                            public ObjectId call() {
                                return atmos.createObject(request).getObjectId();
                            }
                        }, OPERATION_CREATE_OBJECT_FROM_STREAM);
                    }
                }

                // verify preserved object ID
                if (config.isPreserveObjectId()) {
                    if (object.getRelativePath().equals(targetOid.getId())) {
                        log.debug("object ID {} successfully preserved in target", object.getRelativePath());
                    } else {
                        try {
                            delete(targetOid.getId());
                        } catch (Throwable t) {
                            log.warn("could not delete object after failed to preserve OID", t);
                        }
                        throw new RuntimeException(String.format("failed to preserve OID %s (target OID was %s)",
                                object.getRelativePath(), targetOid.getId()));
                    }
                }

                if (targetId == null) targetId = targetOid;

                if (object.isPostStreamUpdateRequired()) updateUserMeta(targetId, object);

                if (options.isSyncRetentionExpiration()) updateRetentionExpiration(targetId, object);

                return targetId.toString();
            } catch (NoSuchAlgorithmException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void updateObject(String identifier, SyncObject object) {
        ObjectIdentifier targetId = config.getAccessType() == namespace ? new ObjectPath(identifier) : new ObjectId(identifier);
        com.emc.atmos.api.bean.ObjectMetadata sourceAtmosMeta = (com.emc.atmos.api.bean.ObjectMetadata) object.getProperty(PROP_ATMOS_METADATA);

        try {
            final Map<String, Metadata> atmosMeta = getAtmosUserMetadata(object.getMetadata());
            Acl atmosAcl = getAtmosAcl(object.getAcl());

            if (object.getMetadata().isDirectory()) { // UPDATE DIRECTORY

                if (options.isSyncMetadata()) updateUserMeta(targetId, object);
                if (options.isSyncAcl()) updateAcl(targetId, object);

            } else { // UPDATE FILE

                if (config.getWsChecksumType() != null || (sourceAtmosMeta != null && sourceAtmosMeta.getWsChecksum() != null)) {
                    // you cannot update a checksummed object; delete and replace.
                    final ObjectIdentifier fTargetId = targetId;
                    time(new Function<Void>() {
                        @Override
                        public Void call() {
                            atmos.delete(fTargetId);
                            return null;
                        }
                    }, OPERATION_DELETE_OBJECT);

                    AtmosConfig.Hash checksumType = config.getWsChecksumType();
                    if (checksumType == null)
                        checksumType = AtmosConfig.Hash.valueOf(sourceAtmosMeta.getWsChecksum().getAlgorithm().toString().toLowerCase());
                    createChecksummedObject(targetId, object, checksumType);

                } else if (options.isSyncData()) {
                    // delete existing metadata if necessary
                    if (config.isReplaceMetadata()) deleteUserMeta(targetId);

                    try (InputStream in = object.getDataStream()) {
                        final UpdateObjectRequest request = new UpdateObjectRequest();

                        if (options.isMonitorPerformance())
                            request.setContent(new ProgressInputStream(in, new PerformanceListener(getWriteWindow())));
                        else request.setContent(in);

                        request.identifier(targetId);
                        if (options.isSyncAcl()) request.acl(atmosAcl);
                        if (options.isSyncMetadata())
                            request.contentType(object.getMetadata().getContentType()).setUserMetadata(atmosMeta.values());
                        request.contentLength(object.getMetadata().getContentLength());
                        time(new Function<Void>() {
                            @Override
                            public Void call() {
                                atmos.updateObject(request);
                                return null;
                            }
                        }, OPERATION_UPDATE_OBJECT_FROM_STREAM);
                    }

                    if (object.isPostStreamUpdateRequired()) updateUserMeta(targetId, object);

                    if (options.isSyncRetentionExpiration()) updateRetentionExpiration(targetId, object);

                } else { // update metadata only

                    if (options.isSyncMetadata()) updateUserMeta(targetId, object);
                    if (options.isSyncAcl()) updateAcl(targetId, object);
                    if (options.isSyncRetentionExpiration()) updateRetentionExpiration(targetId, object);
                }
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException("Failed to store object " + identifier, e);
        }
    }

    private ObjectId createChecksummedObject(ObjectIdentifier targetId, SyncObject obj, AtmosConfig.Hash checksumType)
            throws NoSuchAlgorithmException, IOException {
        Map<String, Metadata> atmosMeta = getAtmosUserMetadata(obj.getMetadata());
        RunningChecksum ck = new RunningChecksum(ChecksumAlgorithm.valueOf(checksumType.toString().toUpperCase()));
        byte[] buffer = new byte[options.getBufferSize()];
        long read = 0;
        int c;
        ObjectId targetOid;

        // create
        final CreateObjectRequest cRequest = new CreateObjectRequest();
        cRequest.identifier(targetId);
        if (options.isSyncAcl()) cRequest.acl(getAtmosAcl(obj.getAcl()));
        if (options.isSyncMetadata())
            cRequest.contentType(obj.getMetadata().getContentType()).setUserMetadata(atmosMeta.values());
        cRequest.wsChecksum(ck);
        // preserve object ID
        if (config.isPreserveObjectId()) cRequest.setCustomObjectId(obj.getRelativePath());
        targetOid = time(new Function<ObjectId>() {
            @Override
            public ObjectId call() {
                return atmos.createObject(cRequest).getObjectId();
            }
        }, OPERATION_CREATE_OBJECT);

        if (options.isSyncData()) {
            try (InputStream in = obj.getDataStream()) {
                while ((c = in.read(buffer)) != -1) {
                    // append
                    ck.update(buffer, 0, c);
                    final UpdateObjectRequest uRequest = new UpdateObjectRequest();
                    uRequest.identifier(targetOid).content(new BufferSegment(buffer, 0, c));
                    uRequest.range(new Range(read, read + c - 1)).wsChecksum(ck);
                    if (options.isSyncMetadata()) uRequest.contentType(obj.getMetadata().getContentType());
                    time(new Function<Object>() {
                        @Override
                        public Object call() {
                            atmos.updateObject(uRequest);
                            return null;
                        }
                    }, OPERATION_UPDATE_OBJECT_FROM_SEGMENT);
                    getWriteWindow().increment(c);
                    read += c;
                }
            }
        }

        return targetOid;
    }

    private void updateUserMeta(final ObjectIdentifier targetId, final SyncObject obj) {
        if (config.isReplaceMetadata()) deleteUserMeta(targetId);
        final Map<String, Metadata> atmosMeta = getAtmosUserMetadata(obj.getMetadata());
        if (atmosMeta != null && atmosMeta.size() > 0) {
            log.debug("Updating metadata on {}", targetId);
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
        final Acl atmosAcl = getAtmosAcl(obj.getAcl());
        if (atmosAcl != null) {
            log.debug("Updating ACL on {}", targetId);
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
            final List<Metadata> retExpList = getExpirationMetadataForUpdate(obj);
            retExpList.addAll(getRetentionMetadataForUpdate(obj));
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
            log.error("Failed to manually set retention/expiration\n" +
                            "(destId: {}, retentionEnd: {}, expiration: {})\n" +
                            "[http: {}, atmos: {}, msg: {}]",
                    destId, Iso8601Util.format(obj.getMetadata().getRetentionEndDate()),
                    Iso8601Util.format(obj.getMetadata().getExpirationDate()),
                    e.getHttpCode(), e.getErrorCode(), e.getMessage());
        } catch (RuntimeException e) {
            log.error("Failed to manually set retention/expiration\n" +
                            "(destId: {}, retentionEnd: {}, expiration: {})\n[error: {}]",
                    destId, Iso8601Util.format(obj.getMetadata().getRetentionEndDate()),
                    Iso8601Util.format(obj.getMetadata().getExpirationDate()), e.getMessage());
        }
    }

    private Acl getAtmosAcl(ObjectAcl objectAcl) {
        Acl acl = new Acl();
        for (String user : objectAcl.getUserGrants().keySet()) {
            for (String permission : objectAcl.getUserGrants().get(user))
                acl.addUserGrant(user, getAtmosPermission(permission));
        }
        for (String group : objectAcl.getGroupGrants().keySet()) {
            for (String permission : objectAcl.getGroupGrants().get(group))
                acl.addGroupGrant(group, getAtmosPermission(permission));
        }

        return acl;
    }

    private Permission getAtmosPermission(String permission) {
        try {
            return Permission.valueOf(permission);
        } catch (IllegalArgumentException e) {
            if (!options.isIgnoreInvalidAcls()) throw e;
            else log.warn("{} does not map to an Atmos ACL permission (you should use the ACL mapper)", permission);
        }
        return null;
    }

    @Override
    public void delete(final String identifier) {
        if (config.isRemoveTagsOnDelete()) {
            // get all tags for the object
            Map<String, Boolean> tags = time(new Function<Map<String, Boolean>>() {
                @Override
                public Map<String, Boolean> call() {
                    return atmos.getUserMetadataNames(getObjectIdentifier(identifier));
                }
            }, OPERATION_GET_USER_META);
            for (final String name : tags.keySet()) {
                // if a tag is listable, delete it
                if (tags.get(name)) time(new Function<Void>() {
                    @Override
                    public Void call() {
                        atmos.deleteUserMetadata(getObjectIdentifier(identifier), name);
                        return null;
                    }
                }, OPERATION_DELETE_USER_META);
            }
        }
        try {
            // delete the object
            time(new Function<Void>() {
                @Override
                public Void call() {
                    atmos.delete(getObjectIdentifier(identifier));
                    return null;
                }
            }, OPERATION_DELETE_OBJECT);
        } catch (AtmosException e) {
            if (e.getErrorCode() == 1023)
                log.warn("could not delete non-empty directory {}", identifier);
            else throw e;
        }
    }

    private List<Metadata> getRetentionMetadataForUpdate(SyncObject object) {
        List<Metadata> list = new ArrayList<>();

        Date retentionEnd = object.getMetadata().getRetentionEndDate();

        if (retentionEnd != null) {
            log.debug("Retention {} (OID: {}, end-date: {})", "enabled",
                    object.getRelativePath(), Iso8601Util.format(retentionEnd));
            list.add(new Metadata("user.maui.retentionEnable", "true", false));
            list.add(new Metadata("user.maui.retentionEnd", Iso8601Util.format(retentionEnd), false));
        }

        return list;
    }

    private List<Metadata> getExpirationMetadataForUpdate(SyncObject object) {
        List<Metadata> list = new ArrayList<>();

        Date expiration = object.getMetadata().getExpirationDate();

        if (expiration != null) {
            log.debug("Expiration {} (OID: {}, end-date: {})", "enabled",
                    object.getRelativePath(), Iso8601Util.format(expiration));
            list.add(new Metadata("user.maui.expirationEnable", "true", false));
            list.add(new Metadata("user.maui.expirationEnd", Iso8601Util.format(expiration), false));
        }

        return list;
    }

    private Map<String, Metadata> getAtmosUserMetadata(ObjectMetadata metadata) {
        Map<String, Metadata> userMetadata = new HashMap<>();
        for (ObjectMetadata.UserMetadata uMeta : metadata.getUserMetadata().values()) {
            userMetadata.put(uMeta.getKey(), new Metadata(uMeta.getKey(), uMeta.getValue(), uMeta.isIndexed()));
        }
        return userMetadata;
    }

    public AtmosApi getAtmos() {
        return atmos;
    }

    private class DirectoryIterator extends ReadOnlyIterator<ObjectSummary> {
        private ObjectPath path;
        private ListDirectoryRequest listRequest;
        private Iterator<DirectoryEntry> atmosIterator;

        DirectoryIterator(ObjectPath path) {
            this.path = path;
            listRequest = new ListDirectoryRequest().path(path).includeMetadata(true);
        }

        @Override
        protected ObjectSummary getNextObject() {
            if (getAtmosIterator().hasNext()) {

                DirectoryEntry entry = getAtmosIterator().next();
                ObjectPath objectPath = new ObjectPath(path, entry);

                Metadata sizeMeta = entry.getSystemMetadataMap().get(SIZE_PROP);
                Metadata typeMeta = entry.getSystemMetadataMap().get(TYPE_PROP);

                return new ObjectSummary(objectPath.getPath(), DIRECTORY_TYPE.equals(typeMeta.getValue()),
                        Long.parseLong(sizeMeta.getValue()));
            }
            return null;
        }

        private synchronized Iterator<DirectoryEntry> getAtmosIterator() {
            if (atmosIterator == null || (!atmosIterator.hasNext() && listRequest.getToken() != null)) {
                atmosIterator = getNextBlock().iterator();
            }
            return atmosIterator;
        }

        private List<DirectoryEntry> getNextBlock() {
            return time(new Function<List<DirectoryEntry>>() {
                @Override
                public List<DirectoryEntry> call() {
                    return atmos.listDirectory(listRequest).getEntries();
                }
            }, OPERATION_LIST_DIRECTORY);
        }
    }
}

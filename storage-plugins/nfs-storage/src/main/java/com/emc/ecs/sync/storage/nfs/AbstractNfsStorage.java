/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.storage.nfs;

import com.emc.ecs.nfsclient.nfs.*;
import com.emc.ecs.nfsclient.nfs.io.NfsFile;
import com.emc.ecs.nfsclient.nfs.io.NfsFilenameFilter;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.storage.NfsConfig;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.AbstractStorage;
import com.emc.ecs.sync.storage.ObjectNotFoundException;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.Iso8601Util;
import com.emc.ecs.sync.util.LazyValue;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import static com.emc.ecs.sync.storage.file.AbstractFilesystemStorage.*;

public abstract class AbstractNfsStorage<C extends NfsConfig, N extends Nfs<F>, F extends NfsFile<N, F>>
        extends AbstractStorage<C> {

    private static Logger log = LoggerFactory.getLogger(AbstractNfsStorage.class);

    public static final String PROP_FILE = "nfs.file";

    private Date modifiedSince;
    private List<Pattern> excludedPathPatterns;
    private String identifierBase;
    private F syncRootFile;

    private final MimetypesFileTypeMap mimeMap;
    private final NfsFilenameFilter filter;

    /**
     * Only constructor.
     */
    @SuppressWarnings("rawtypes")
    public AbstractNfsStorage() {
        mimeMap = new MimetypesFileTypeMap();
        filter = new AbstractNfsStorage.SourceFilter();
    }

    /**
     * Provides an InputStream for the nfsFile.
     * 
     * @param nfsFile 
     * @return the stream
     * @throws IOException
     */
    protected abstract InputStream createInputStream(F nfsFile) throws IOException;

    /**
     * Provides an OutputStream for the nfsFile.
     * 
     * @param nfsFile
     * @return the stream
     * @throws IOException
     */
    protected abstract OutputStream createOutputStream(F nfsFile) throws IOException;

    /**
     * Provides an nfsFile for an arbitrary identifier.
     * 
     * @param identifier
     * @return the nfsFile
     * @throws IOException
     */
    protected abstract F createFile(String identifier) throws IOException;

    /**
     * Provides an nfsFile with the given name in the parent directory.
     * 
     * @param parent the parent directory
     * @param childName the name
     * @return the nfsFile child in the parent directory
     * @throws IOException
     */
    protected abstract F createFile(F parent, String childName) throws IOException;

    /**
     * Provides a way to get files directly from the path, to be used in configure() only.
     * 
     * @param path
     * @return the created NFS file;
     * @throws IOException 
     */
    protected abstract F createFileFromPath(String path) throws IOException;

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.SyncStorage#getRelativePath(java.lang.String, boolean)
     */
    public String getRelativePath(String identifier, boolean directory) {
        return getRelativePath(identifier, identifierBase);
    }

    /**
     * Return the path relative to the base, using NFS (Unix/Linux) path conventions.
     * 
     * @param path the path
     * @param pathBase the base
     * @return the relative path
     */
    protected String getRelativePath(String path, String pathBase) {
        String relativePath = path.startsWith(pathBase) ? path.substring(pathBase.length()) : path;
        while (relativePath.startsWith(NfsFile.separator)) {
            relativePath = relativePath.substring(1);
        }
        return relativePath;
    }

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.SyncStorage#getIdentifier(java.lang.String, boolean)
     */
    public String getIdentifier(String relativePath, boolean directory) {
        try {
            return StringUtils.isBlank(relativePath) ? identifierBase : syncRootFile.getChildFile(relativePath).getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Return the full path, using NFS (Unix/Linux) path conventions, except for null/empty strings where we use ecs-sync conventions.
     * 
     * @param pathBase the base
     * @param relativePath the relative path
     * @return the full path
     */
    protected String combineWithFileSeparator(String pathBase, String relativePath) {
        if (pathBase == null) {
            pathBase = NfsFile.separator;
        }

        if ((relativePath == null) || ("".equals(relativePath))) {
            return pathBase;
        }

        if ( !( pathBase.endsWith( NfsFile.separator ) ) ) {
            pathBase = pathBase + NfsFile.separator;
        }

        while ( relativePath.startsWith( NfsFile.separator ) ) {
            relativePath = relativePath.substring(1);
        }

        return pathBase + relativePath;
    }

    
    /* (non-Javadoc)
     * @see com.emc.ecs.sync.AbstractPlugin#configure(com.emc.ecs.sync.storage.SyncStorage, java.util.Iterator, com.emc.ecs.sync.storage.SyncStorage)
     */
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        syncRootFile = null;
        identifierBase = "";
        try {
            F mountPathRoot = createFileFromPath("");
            if (!mountPathRoot.exists()) {
                throw new ConfigurationException("the mount " + mountPathRoot + " is unavailable.");
            }
            syncRootFile = createFileFromPath(config.getSubPath());
            identifierBase = syncRootFile.getAbsolutePath();
        } catch (IOException e) {
            throw new ConfigurationException(e);
        }

        if (source == this) {
            try {
                if (!syncRootFile.exists()) {
                    throw new ConfigurationException("the source " + syncRootFile + " does not exist.");
                }
            } catch (IOException e) {
                throw new ConfigurationException(e);
            }

            if (config.getModifiedSince() != null) {
                modifiedSince = Iso8601Util.parse(config.getModifiedSince());
                if (modifiedSince == null) {
                    throw new ConfigurationException("could not parse modified-since");
                }
            }

            if (config.getExcludedPaths() != null) {
                excludedPathPatterns = new ArrayList<>();
                for (String pattern : config.getExcludedPaths()) {
                    excludedPathPatterns.add(Pattern.compile(pattern));
                }
            }
        }

    }

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.AbstractStorage#createSummary(java.lang.String)
     */
    protected ObjectSummary createSummary(String identifier) throws ObjectNotFoundException {
        try {
            return createSummary(createFile(identifier));
        } catch (IOException e) {
            throw new ObjectNotFoundException(e);
        }
    }

    /**
     * Create an ObjectSummary for the nfsFile.
     * 
     * @param nfsFile the nfsFile
     * @return the summary
     */
    private ObjectSummary createSummary(F nfsFile) {
        try {
            if (!nfsFile.exists()) {
                throw new ObjectNotFoundException(nfsFile.getPath());
            }
            boolean link = isSymLink(nfsFile);
            boolean directory = nfsFile.isDirectory() && (config.isFollowLinks() || !link);
            long size = directory || link ? 0 : nfsFile.length();
            return new ObjectSummary(nfsFile.getAbsolutePath(), directory, size);
        } catch (IOException e) {
            throw new ConfigurationException(e);
        }
    }

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.SyncStorage#allObjects()
     */
    public Iterable<ObjectSummary> allObjects() {
        ObjectSummary syncRoot = createSummary(getIdentifier("", true));
        return syncRoot.isDirectory() ? children(syncRoot) : Collections.singletonList(syncRoot);
    }

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.SyncStorage#children(com.emc.ecs.sync.model.ObjectSummary)
     */
    public List<ObjectSummary> children(ObjectSummary parent) {
        try {
            List<ObjectSummary> entries = new ArrayList<>();
            List<F> nfsFiles = createFile(parent.getIdentifier()).listFiles(filter);
            if (nfsFiles != null) {
                for (F nfsFile : nfsFiles) {
                    entries.add(createSummary(nfsFile));
                }
            }
            return entries;
        } catch (IOException e) {
            throw new ConfigurationException(e);
        }
    }

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.SyncStorage#loadObject(java.lang.String)
     */
    public SyncObject loadObject(final String identifier) throws ObjectNotFoundException {
        final F nfsFile;
        final boolean isDirectory;
        ObjectMetadata metadata;
        try {
            nfsFile = createFile(identifier);
            isDirectory = nfsFile.isDirectory();
            metadata = readMetadata(nfsFile);
        } catch (IOException e) {
            throw new ObjectNotFoundException(e);
        }

        LazyValue<InputStream> lazyStream = new LazyValue<InputStream>() {
            @Override
            public InputStream get() {
                return readDataStream(nfsFile);
            }
        };
        LazyValue<ObjectAcl> lazyAcl = new LazyValue<ObjectAcl>() {
            @Override
            public ObjectAcl get() {
                return readAcl(nfsFile);
            }
        };

        SyncObject object = new SyncObject(this, getRelativePath(identifier, isDirectory), metadata).withLazyStream(lazyStream)
                .withLazyAcl(lazyAcl);
        object.setProperty(PROP_FILE, nfsFile);
        return object;
    }

    /**
     * Return the ObjectMetadata, generating it if necessary.
     * 
     * @param nfsFile the nfsFile
     * @return the metadata
     * @throws IOException
     */
    private ObjectMetadata readMetadata(F nfsFile) throws IOException {
        ObjectMetadata metadata;
        try {
            // first try to load the metadata file
            metadata = readMetadataFile(nfsFile);
        } catch (Throwable t) {
            // if that doesn't work, generate new metadata based on the file
            // attributes
            metadata = new ObjectMetadata();

            boolean isLink = !config.isFollowLinks() && isSymLink(nfsFile);
            boolean directory = nfsFile.isDirectory();

            metadata.setDirectory(directory);

            NfsGetAttributes basicAttr = nfsFile.getattr().getAttributes();
            NfsTime mtime = basicAttr.getMtime();
            long mtimeInMillis = (mtime == null) ? 0 : mtime.getTimeInMillis();
            metadata.setModificationTime(new Date(mtimeInMillis));
            NfsTime atime = basicAttr.getAtime();
            long atimeInMillis = (atime == null) ? 0 : atime.getTimeInMillis();
            metadata.setAccessTime(new Date(atimeInMillis));
            NfsTime ctime = basicAttr.getCtime();
            long ctimeInMillis = (ctime == null) ? 0 : ctime.getTimeInMillis();
            metadata.setMetaChangeTime(new Date(ctimeInMillis));

            metadata.setContentType(isLink ? TYPE_LINK : mimeMap.getContentType(nfsFile.getName()));
            if (isLink)
                metadata.setUserMetadataValue(META_LINK_TARGET, nfsFile.readlink().getData());

            // On OSX, directories have 'length'... ignore.
            if (nfsFile.isFile() && !isLink)
                metadata.setContentLength(nfsFile.length());
            else
                metadata.setContentLength(0);
        }

        return metadata;
    }

    /**
     * Get previously stored metadata.
     * 
     * @param nfsFile the nfsFile
     * @return the metadata from the nfsFile
     * @throws IOException
     */
    private ObjectMetadata readMetadataFile(F nfsFile) throws IOException {
        try (InputStream is = new BufferedInputStream(createInputStream(getMetaFile(nfsFile)))) {
            return ObjectMetadata.fromJson(new Scanner(is).useDelimiter("\\A").next());
        }
    }

    /**
     * Get the data.
     * 
     * @param nfsFile the nfsFile
     * @return the stream
     */
    private InputStream readDataStream(F nfsFile) {
        try {
            return createInputStream(nfsFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create an ObjectAcl for the nfsFile.
     * 
     * @param nfsFile the nfsFile
     * @return the acl
     */
    protected ObjectAcl readAcl(F nfsFile) {
        NfsGetAttributes attributes = null;
        try {
            attributes = nfsFile.getAttributes();
        } catch (Throwable t) {
            throw new RuntimeException("could not read nfsFile ACL", t);
        }

        ObjectAcl acl = new ObjectAcl();
        acl.setOwner("uid:" + attributes.getUid());
        String group = "gid:" + attributes.getGid();
        long mode = attributes.getMode();

        if ((mode & NfsFile.ownerReadModeBit) > 0) {
            acl.addUserGrant(acl.getOwner(), READ);
        }
        if ((mode & NfsFile.ownerWriteModeBit) > 0) {
            acl.addUserGrant(acl.getOwner(), WRITE);
        }
        if ((mode & NfsFile.ownerExecuteModeBit) > 0) {
            acl.addUserGrant(acl.getOwner(), EXECUTE);
        }

        if ((mode & NfsFile.groupReadModeBit) > 0) {
            acl.addGroupGrant(group, READ);
        }
        if ((mode & NfsFile.groupWriteModeBit) > 0) {
            acl.addGroupGrant(group, WRITE);
        }
        if ((mode & NfsFile.groupExecuteModeBit) > 0) {
            acl.addGroupGrant(group, EXECUTE);
        }

        if ((mode & NfsFile.othersReadModeBit) > 0) {
            acl.addGroupGrant(OTHER_GROUP, READ);
        }
        if ((mode & NfsFile.othersWriteModeBit) > 0) {
            acl.addGroupGrant(OTHER_GROUP, WRITE);
        }
        if ((mode & NfsFile.othersExecuteModeBit) > 0) {
            acl.addGroupGrant(OTHER_GROUP, EXECUTE);
        }

        return acl;
    }

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.SyncStorage#updateObject(java.lang.String, com.emc.ecs.sync.model.SyncObject)
     */
    public void updateObject(String identifier, SyncObject object) {
        try {
            F nfsFile = createFile(identifier);
            writeFile(nfsFile, object, options.isSyncData());
            if (options.isSyncMetadata()) {
                writeMetadata(nfsFile, object.getMetadata());
            }
            if (options.isSyncAcl()) {
                writeAcl(nfsFile, object.getAcl());
            }
            object.setProperty(PROP_FILE, nfsFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Synchronized mkdirs to prevent conflicts in threaded environment.
     * 
     * @param dir the nfsFile for the directory
     * @throws IOException
     */
    private synchronized void mkdirs(F dir) throws IOException {
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    /**
     * Sync the nfsFile.
     * 
     * @param nfsFile the nfsFile
     * @param object the SyncObject
     * @param streamData whether to sync data after nfsFile creation
     * @throws IOException
     */
    private void writeFile(F nfsFile, SyncObject object, boolean streamData) throws IOException {
        // make sure parent directory exists
        mkdirs(nfsFile.getParentFile());

        if (object.getMetadata().isDirectory()) {
            mkdirs(nfsFile);
        } else if (TYPE_LINK.equals(object.getMetadata().getContentType())) { // restore
                                                                              // a
                                                                              // sym
                                                                              // link
            String targetPath = object.getMetadata().getUserMetadataValue(META_LINK_TARGET);
            if (targetPath == null) {
                throw new RuntimeException("object appears to be a symbolic link, but no target path was found");
            }
            log.info("re-creating symbolic link {} -> {}", object.getRelativePath(), targetPath);
            createSymLink(nfsFile, targetPath);
        } else if (streamData) {
            copyData(object.getDataStream(), nfsFile);
        } else {
            nfsFile.createNewFile();
        }
    }

    /**
     * Create a symlink if needed.
     * 
     * @param nfsFile the nfsFile
     * @param targetPath the symlink target
     * @throws IOException
     */
    private void createSymLink(F nfsFile, String targetPath) throws IOException {
        boolean needNewSymLink = true;
        try {
            if ((nfsFile.getAttributes().getType() == NfsType.NFS_LNK) && targetPath.equals(nfsFile.readlink().getData())) {
                needNewSymLink = false;
            } else {
                nfsFile.delete();
            }
        } catch (Throwable t) {
            // do nothing, this is normal if the nfsFile doesn't exist.
        }
        if (needNewSymLink) {
            nfsFile.symlink(targetPath, new NfsSetAttributes());
        }
    }

    /**
     * Write metadata as needed.
     * 
     * @param nfsFile the nfsFile
     * @param metadata the ObjectMetadata
     * @throws IOException
     */
    private void writeMetadata(F nfsFile, ObjectMetadata metadata) throws IOException {
        if (config.isStoreMetadata()) {
            F metaFile = getMetaFile(nfsFile);
            F metaDir = metaFile.getParentFile();

            // create metadata directory if it doesn't already exist
            synchronized (this) {
                if (!metaDir.exists()) {
                    metaDir.mkdirs();
                }
            }

            String metaJson = metadata.toJson();
            copyData(new ByteArrayInputStream(metaJson.getBytes("UTF-8")), metaFile);
        }

        // write nfsFilesystem metadata (times)
        Date mtime = metadata.getModificationTime();
        if (mtime != null) {
            nfsFile.setLastModified(mtime.getTime());
        }

        Date atime = metadata.getAccessTime();
        if (atime != null) {
            NfsSetAttributes atimeAttributes = new NfsSetAttributes();
            atimeAttributes.setAtime(new NfsTime(atime.getTime()));
            nfsFile.setAttributes(atimeAttributes);
        }
    }

    /**
     * Set the access permissions appropriately.
     * 
     * @param nfsFile the nfsFile
     * @param acl the ObjectAcl
     */
    protected void writeAcl(F nfsFile, ObjectAcl acl) {
        String ownerName = null;
        String groupOwnerName = null;
        long mode = 0;

        if (acl != null) {
            // extract the group owner. since SyncAcl does not provide the group
            // owner directly, take the first group in
            // the grant list that's not "other"
            for (String groupName : acl.getGroupGrants().keySet()) {
                if (groupName.equals(OTHER_GROUP)) {
                    // add all "other" permissions
                    for (String grant : acl.getGroupGrants().get(groupName)) {
                        if (READ.equals(grant)) {
                            mode |= NfsFile.othersReadModeBit;
                        } else if (WRITE.equals(grant)) {
                            mode |= NfsFile.othersWriteModeBit;
                        } else if (EXECUTE.equals(grant)) {
                            mode |= NfsFile.othersExecuteModeBit;
                        }
                    }
                } else if (groupOwnerName == null) {
                    groupOwnerName = groupName;
                    // add group owner permissions
                    for (String grant : acl.getGroupGrants().get(groupName)) {
                        if (READ.equals(grant)) {
                            mode |= NfsFile.groupReadModeBit;
                        } else if (WRITE.equals(grant)) {
                            mode |= NfsFile.groupWriteModeBit;
                        } else if (EXECUTE.equals(grant)) {
                            mode |= NfsFile.groupExecuteModeBit;
                        }
                    }
                }
            }

            ownerName = acl.getOwner();
            if (ownerName == null) {
                for (String userName : acl.getUserGrants().keySet()) {
                    if (userName != null) {
                        ownerName = userName;
                        break;
                    }
                }
            }

            if (ownerName != null) {
                // add owner permissions
                for (String grant : acl.getUserGrants().get(ownerName)) {
                    if (READ.equals(grant)) {
                        mode |= NfsFile.ownerReadModeBit;
                    } else if (WRITE.equals(grant)) {
                        mode |= NfsFile.ownerWriteModeBit;
                    } else if (EXECUTE.equals(grant)) {
                        mode |= NfsFile.ownerExecuteModeBit;
                    }
                }
            }
        }

        try {
            NfsSetAttributes attributes = new NfsSetAttributes();
            attributes.setMode(mode);
            if (ownerName != null && ownerName.startsWith("uid:")) { // we currently only support UID
                attributes.setUid(Long.parseLong(ownerName.substring(4)));
            } else {
                log.info("{} does not have a UID assigned", nfsFile.getPath());
            }
            if (groupOwnerName != null && groupOwnerName.startsWith("gid:")) { // we currently only support GID
                attributes.setGid(Long.parseLong(groupOwnerName.substring(4)));
            } else {
                log.info("{} does not have a GID assigned", nfsFile.getPath());
            }

            nfsFile.setAttributes(attributes);
        } catch (IOException e) {
            throw new RuntimeException("could not write nfsFile attributes for " + nfsFile.getPath(), e);
        }
    }

    /**
     * Copy the data to the nfsFile.
     * 
     * @param inStream the data
     * @param nfsFile the nfsFile
     * @throws IOException
     */
    private void copyData(InputStream inStream, F nfsFile) throws IOException {
        byte[] buffer = new byte[options.getBufferSize()];
        int c;
        try (InputStream input = inStream; OutputStream output = createOutputStream(nfsFile)) {
            while ((c = input.read(buffer)) != -1) {
                output.write(buffer, 0, c);
                if (options.isMonitorPerformance())
                    getWriteWindow().increment(c);
            }
        }
    }

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.AbstractStorage#delete(java.lang.String)
     */
    //TODO: make sure that the source object has not been modified since it was copied to the target, before deleting
    @Override
    public void delete(String identifier, SyncObject object) {
        try {
            delete(identifier, config.getDeleteOlderThan());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Delete the nfsFile.
     * @param identifier the nfsFile identifier
     * @param deleteOlderThan the minimum age for deletion in milliseconds, or 0 if none.
     * @throws IOException
     */
    public void delete(String identifier, long deleteOlderThan) throws IOException {
        F objectFile = createFile(identifier);
        F metaFile = getMetaFile(objectFile);
        if (metaFile.exists()) {
            delete(metaFile, deleteOlderThan);
        }
        delete(objectFile, deleteOlderThan);
    }

    /**
     * Delete the nfsFile.
     * @param nfsFile the nfsFile
     * @param deleteOlderThan the minimum age for deletion in milliseconds, or 0 if none.
     * @throws IOException
     */
    protected void delete(F nfsFile, long deleteOlderThan) throws IOException {
        if (nfsFile.isDirectory()) {
            synchronized (this) {
                F metaDir = getMetaFile(nfsFile).getParentFile();
                try {
                    if (metaDir.exists()) {
                        metaDir.delete();
                    }
                } catch (IOException e) {
                    log.warn("failed to delete metaDir {}", metaDir);
                }
                // Just try and delete dir
                try {
                    nfsFile.delete();
                } catch (IOException e) {
                    log.warn("failed to delete directory {}", nfsFile);
                }
            }
        } else {
            // Must make sure to throw exceptions when necessary to flag
            // actual
            // failures as opposed to skipped files.
            if ((deleteOlderThan > 0) && ((System.currentTimeMillis() - nfsFile.lastModified()) < deleteOlderThan)) {
                log.info("not deleting {}; it is not at least {} ms old", nfsFile, deleteOlderThan);
            } else {
                log.debug("deleting {}", nfsFile);
                nfsFile.delete();
            }
        }
    }

    /**
     * Get the nfsFile holding the metadata for this nfsFile.
     * 
     * @param nfsFile the nfsFile
     * @return the nfsFile holding the metadata
     * @throws IOException
     */
    private F getMetaFile(F nfsFile) throws IOException {
        try {
            if (!nfsFile.isDirectory()) {
                return nfsFile.getParentFile().newChildFile(ObjectMetadata.METADATA_DIR).newChildFile(nfsFile.getName());
            } else {
                return nfsFile.newChildFile(ObjectMetadata.METADATA_DIR).newChildFile(ObjectMetadata.DIR_META_FILE);
            }
        } catch (IOException e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Is the nfsFile a symbolic link?
     * 
     * @param nfsFile the nfsFile
     * @return true if it is, false if not
     * @throws IOException
     */
    private boolean isSymLink(F nfsFile) throws IOException {
        return nfsFile.getattr().getAttributes().getType() == NfsType.NFS_LNK;
    }

    /**
     * Get the filter.
     * 
     * @return the filter
     */
    public NfsFilenameFilter getFilter() {
        return filter;
    }

    private class SourceFilter implements NfsFilenameFilter {

        /* (non-Javadoc)
         * @see com.emc.ecs.nfsclient.nfs.io.NfsFilenameFilter#accept(com.emc.ecs.nfsclient.nfs.io.NfsFile, java.lang.String)
         */
        @SuppressWarnings("unchecked")
        public boolean accept(NfsFile<?, ?> dir, String childName) {
            if (ObjectMetadata.METADATA_DIR.equals(childName) || ObjectMetadata.DIR_META_FILE.equals(childName))
                return false;

            F target;
            try {
                target = createFile((F) dir, childName);
            } catch (IOException e) {
                log.warn("could not get attributes for " + dir.getPath() + NfsFile.separator + childName, e);
                return false;
            }

            // exclude paths filter
            if (excludedPathPatterns != null) {
                for (Pattern p : excludedPathPatterns) {
                    if (p.matcher(target.getPath()).matches()) {
                        if (log.isDebugEnabled())
                            log.debug("skipping nfsFile {}: matches pattern: {}", target, p);
                        return false;
                    }
                }
            }

            // modified since filter
            if (modifiedSince != null) {
                try {
                    if (config.isFollowLinks()) {
                        target = target.followLinks();
                    }
                    NfsGetAttributes attributes = target.getAttributes();
                    if ((NfsType.NFS_DIR != attributes.getType()) && hasNotBeenModifiedSince(attributes)) {
                        return false;
                    }
                } catch (IOException e) {
                    log.warn("could not get attributes for " + target.getPath(), e);
                    return false;
                }
            }

            return true;
        }

        /**
         * Read the attributes and determine whether the nfsFile has been modified since the indicated time.
         * 
         * @param attributes the attributes
         * @return true if it has, false otherwise.
         */
        private boolean hasNotBeenModifiedSince(NfsGetAttributes attributes) {
            return attributes.getMtime().getTimeInMillis() <= modifiedSince.getTime();
        }
    }

}

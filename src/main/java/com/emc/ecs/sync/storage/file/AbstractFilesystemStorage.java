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
package com.emc.ecs.sync.storage.file;

import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

public abstract class AbstractFilesystemStorage<C extends FilesystemConfig> extends AbstractStorage<C> {
    private static Logger log = LoggerFactory.getLogger(AbstractFilesystemStorage.class);

    public static final String PROP_FILE = "filesystem.file";

    public static final String OTHER_GROUP = "other";

    public static final String READ = "READ";
    public static final String WRITE = "WRITE";
    public static final String EXECUTE = "EXECUTE";

    public static final String TYPE_LINK = "application/x-symlink";
    public static final String META_LINK_TARGET = "x-emc-link-target";

    private static final String OPERATION_WRITE_DATA = "FilesystemWriteObjectData";
    private static final String OPERATION_WRITE_METADATA = "FilesystemWriteObjectMetadata";

    private Date modifiedSince;
    private List<Pattern> excludedPathPatterns;

    private MimetypesFileTypeMap mimeMap;
    private SourceFilter filter;

    protected AbstractFilesystemStorage() {
        mimeMap = new MimetypesFileTypeMap();
        filter = new SourceFilter();
    }

    /**
     * Implement to provide an InputStream implementation (i.e. TFileInputStream)
     */
    protected abstract InputStream createInputStream(File f) throws IOException;

    /**
     * Implement to provide an OutputStream implementation (i.e. TFileOutputStream)
     */
    protected abstract OutputStream createOutputStream(File f) throws IOException;

    /**
     * Implement to provide a File implementation (i.e. TFile)
     */
    public abstract File createFile(String path);

    /**
     * Implement to provide a File implementation (i.e. TFile)
     */
    public abstract File createFile(File parent, String path);

    private File createFile(String parent, String path) {
        return createFile(createFile(parent), path);
    }

    @Override
    public String getRelativePath(String identifier, boolean directory) {
        String relativePath = createFile(identifier).getAbsolutePath();
        File rootFile = createFile(config.getPath());
        if (!config.isUseAbsolutePath() && relativePath.startsWith(rootFile.getAbsolutePath())) {
            relativePath = relativePath.substring(rootFile.getAbsolutePath().length());
        }
        if (File.separatorChar == '\\') {
            relativePath = relativePath.replace('\\', '/');
        }
        if (relativePath.startsWith("/")) {
            relativePath = relativePath.substring(1);
        }
        return relativePath;
    }

    @Override
    public String getIdentifier(String relativePath, boolean directory) {
        return createFile(config.getPath(), relativePath).getPath();
    }

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        File rootFile = createFile(config.getPath());

        if (source == this) {
            if (!rootFile.exists())
                throw new ConfigurationException("the source " + rootFile + " does not exist");

            if (config.getModifiedSince() != null) {
                modifiedSince = Iso8601Util.parse(config.getModifiedSince());
                if (modifiedSince == null) throw new ConfigurationException("could not parse modified-since");
            }

            if (config.getDeleteCheckScript() != null) {
                File deleteCheckScript = new File(config.getDeleteCheckScript());
                if (!deleteCheckScript.exists())
                    throw new ConfigurationException("delete check script " + deleteCheckScript + " does not exist");
            }

            if (config.getExcludedPaths() != null) {
                excludedPathPatterns = new ArrayList<>();
                for (String pattern : config.getExcludedPaths()) {
                    excludedPathPatterns.add(Pattern.compile(pattern));
                }
            }
        }
    }

    @Override
    protected ObjectSummary createSummary(String identifier) {
        return createSummary(createFile(identifier));
    }

    private ObjectSummary createSummary(File file) {
        boolean link = isSymLink(file);
        boolean directory = file.isDirectory() && (config.isFollowLinks() || !link);
        long size = directory || link ? 0 : file.length();
        return new ObjectSummary(file.getPath(), directory, size);
    }

    @Override
    public Iterable<ObjectSummary> allObjects() {
        ObjectSummary rootSummary = createSummary(config.getPath());
        if (rootSummary.isDirectory() && !config.isIncludeBaseDir()) return children(rootSummary);
        else return Collections.singletonList(rootSummary);
    }

    @Override
    public List<ObjectSummary> children(ObjectSummary parent) {
        List<ObjectSummary> entries = new ArrayList<>();
        // must use NIO here to make sure we get an exception
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(createFile(parent.getIdentifier()).toPath(), filter)) {
            for (Path path : stream) {
                entries.add(createSummary(path.toFile()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return entries;
    }

    @Override
    public SyncObject loadObject(final String identifier) throws ObjectNotFoundException {
        ObjectMetadata metadata = readMetadata(identifier);

        LazyValue<InputStream> lazyStream = new LazyValue<InputStream>() {
            @Override
            public InputStream get() {
                return readDataStream(identifier);
            }
        };
        LazyValue<ObjectAcl> lazyAcl = new LazyValue<ObjectAcl>() {
            @Override
            public ObjectAcl get() {
                return readAcl(identifier);
            }
        };

        SyncObject object = new SyncObject(this, getRelativePath(identifier, metadata.isDirectory()), metadata)
                .withLazyStream(lazyStream).withLazyAcl(lazyAcl);
        object.setProperty(PROP_FILE, createFile(identifier));
        return object;
    }

    private ObjectMetadata readMetadata(String identifier) {
        File file = createFile(identifier);

        if (!Files.exists(file.toPath(), getLinkOptions())) throw new ObjectNotFoundException(identifier);

        ObjectMetadata metadata;
        try {
            // first try to load the metadata file
            metadata = readMetadataFile(file);
        } catch (Throwable t) {
            // if that doesn't work, generate new metadata based on the file attributes
            metadata = new ObjectMetadata();

            boolean isLink = !config.isFollowLinks() && isSymLink(file);
            boolean directory = Files.isDirectory(file.toPath(), getLinkOptions());

            BasicFileAttributes basicAttr = readAttributes(file);

            metadata.setDirectory(directory);

            FileTime mtime = basicAttr.lastModifiedTime();
            metadata.setModificationTime(new Date(mtime.toMillis()));
            FileTime atime = basicAttr.lastAccessTime();
            metadata.setAccessTime(new Date(atime.toMillis()));

            metadata.setContentType(isLink ? TYPE_LINK : mimeMap.getContentType(file));
            if (isLink) {
                String linkTarget = getLinkTarget(file);
                metadata.setUserMetadataValue(META_LINK_TARGET, linkTarget);
                // helpful logging for link visibility
                log.info("storing symbolic link {} -> {}", identifier, linkTarget);
            }

            // On OSX, directories have 'length'... ignore.
            if (file.isFile() && !isLink) metadata.setContentLength(file.length());
            else metadata.setContentLength(0);
        }

        return metadata;
    }

    private ObjectMetadata readMetadataFile(File objectFile) throws IOException {
        try (InputStream is = new BufferedInputStream(createInputStream(getMetaFile(objectFile)))) {
            return ObjectMetadata.fromJson(new Scanner(is).useDelimiter("\\A").next());
        }
    }

    private InputStream readDataStream(String identifier) {
        try {
            File file = createFile(identifier);
            if (!config.isFollowLinks() && isSymLink(file)) return new ByteArrayInputStream(new byte[0]);
            else return createInputStream(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: make this windows-compatible
    protected ObjectAcl readAcl(String identifier) {
        PosixFileAttributes attributes;
        Integer uid, gid;
        try {
            BasicFileAttributes basicAttrs = readAttributes(createFile(identifier));
            if (!(basicAttrs instanceof PosixFileAttributes)) {
                // Can't handle.  Return empty ACL.
                return new ObjectAcl();
            }
            attributes = (PosixFileAttributes) basicAttrs;
            File file = createFile(identifier);
            uid = (Integer) Files.getAttribute(file.toPath(), "unix:uid", getLinkOptions());
            gid = (Integer) Files.getAttribute(file.toPath(), "unix:gid", getLinkOptions());
        } catch (Throwable t) {
            throw new RuntimeException("could not read file ACL", t);
        }

        ObjectAcl acl = new ObjectAcl();

        if (uid != null) acl.setOwner("uid:" + uid);
        else if (attributes.owner() != null) acl.setOwner(attributes.owner().getName());

        String group = null;
        if (gid != null) group = "gid:" + gid.toString();
        else if (attributes.group() != null) group = attributes.group().getName();

        for (PosixFilePermission permission : attributes.permissions()) {
            switch (permission) {
                case OWNER_READ:
                case OWNER_WRITE:
                case OWNER_EXECUTE:
                    if (acl.getOwner() != null) acl.addUserGrant(acl.getOwner(), fromPosixPermission(permission));
                    break;
                case GROUP_READ:
                case GROUP_WRITE:
                case GROUP_EXECUTE:
                    if (group != null) acl.addGroupGrant(group, fromPosixPermission(permission));
                    break;
                case OTHERS_READ:
                case OTHERS_WRITE:
                case OTHERS_EXECUTE:
                    acl.addGroupGrant(OTHER_GROUP, fromPosixPermission(permission));
                    break;
            }
        }

        return acl;
    }

    @Override
    public void updateObject(String identifier, SyncObject object) {
        File file = createFile(identifier);

        writeFile(file, object, options.isSyncData());
        if (options.isSyncMetadata()) writeMetadata(file, object.getMetadata());
        if (options.isSyncAcl()) writeAcl(file, object.getAcl());
    }

    private void writeFile(File file, SyncObject object, boolean streamData) {
        Path path = file.toPath();

        // make sure parent directory exists
        mkdirs(file.getParentFile());

        if (object.getMetadata().isDirectory()) {
            try {
                mkdir(file);
            } catch (IOException e) {
                throw new RuntimeException("failed to create directory " + file, e);
            }
        } else {
            try {
                if (TYPE_LINK.equals(object.getMetadata().getContentType())) { // restore a sym link
                    String targetPath = object.getMetadata().getUserMetadataValue(META_LINK_TARGET);
                    if (targetPath == null)
                        throw new RuntimeException("object appears to be a symbolic link, but no target path was found");

                    if (Files.exists(path)) {
                        if (!isSymLink(file))
                            throw new RuntimeException("target exists and is not a sym link (source is a sym link)");
                        if (!targetPath.equals(getLinkTarget(file))) {
                            log.info("overwriting symbolic link {} -> {}", object.getRelativePath(), targetPath);
                            Files.delete(path);
                            Files.createSymbolicLink(path, Paths.get(targetPath));
                        }
                    } else {
                        log.info("creating symbolic link {} -> {}", object.getRelativePath(), targetPath);
                        Files.createSymbolicLink(path, Paths.get(targetPath));
                    }
                } else {
                    if (streamData) {
                        time((Callable<Void>) () -> {
                            copyData(object.getDataStream(), file);
                            return null;
                        }, OPERATION_WRITE_DATA);
                    } else if (!Files.isRegularFile(path)) {
                        Files.createFile(path);
                    }
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("error writing: " + file, e);
            }
        }
    }

    private void writeMetadata(File file, ObjectMetadata metadata) {
        if (config.isStoreMetadata()) {
            File metaFile = getMetaFile(file);
            File metaDir = metaFile.getParentFile();

            // create metadata directory if it doesn't already exist
            try {
                mkdir(metaDir);
            } catch (IOException e) {
                throw new RuntimeException("failed to create metadata directory " + metaDir, e);
            }

            try {
                String metaJson = metadata.toJson();
                time((Callable<Void>) () -> {
                    copyData(new ByteArrayInputStream(metaJson.getBytes("UTF-8")), metaFile);
                    return null;
                }, OPERATION_WRITE_METADATA);
            } catch (Exception e) {
                throw new RuntimeException("failed to write metadata to: " + metaFile, e);
            }
        }

        // write filesystem metadata (times)
        Date mtime = metadata.getModificationTime();
        Date atime = metadata.getAccessTime();
        if ((atime != null || mtime != null) && !isSymLink(file)) { // cannot set times for symlinks in Java
            try {
                BasicFileAttributeView view = Files.getFileAttributeView(file.toPath(), BasicFileAttributeView.class);
                FileTime mtimeT = null, atimeT = null;
                if (mtime != null) mtimeT = FileTime.fromMillis(mtime.getTime());
                if (atime != null) atimeT = FileTime.fromMillis(atime.getTime());
                view.setTimes(mtimeT, atimeT, null);
            } catch (IOException e) {
                throw new RuntimeException("failed to set file times on " + file, e);
            }
        }
    }

    protected void writeAcl(File file, ObjectAcl acl) {
        String ownerName = null;
        String groupOwnerName = null;
        Set<PosixFilePermission> permissions = null;

        if (acl != null) {
            permissions = new HashSet<>();

            // extract the group owner. since SyncAcl does not provide the group owner directly, take the first group in
            // the grant list that's not "other"
            for (String groupName : acl.getGroupGrants().keySet()) {
                if (groupName.equals(OTHER_GROUP)) {
                    // add all "other" permissions
                    permissions.addAll(getPosixPermissions(acl.getGroupGrants().get(groupName), PosixType.OTHER));
                } else if (groupOwnerName == null) {
                    groupOwnerName = groupName;
                    // add group owner permissions
                    permissions.addAll(getPosixPermissions(acl.getGroupGrants().get(groupName), PosixType.GROUP));
                }
            }

            ownerName = acl.getOwner();
            for (String userName : acl.getUserGrants().keySet()) {
                if (ownerName == null) ownerName = userName;
                if (ownerName.equals(userName)) {
                    // add owner permissions
                    permissions.addAll(getPosixPermissions(acl.getUserGrants().get(userName), PosixType.OWNER));
                }
            }
        }

        try {
            PosixFileAttributeView attributeView = Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class);
            UserPrincipalLookupService lookupService = file.toPath().getFileSystem().getUserPrincipalLookupService();

            // set permission bits first (might not be able to after setting ownership)
            if (permissions != null) attributeView.setPermissions(permissions);

            if (ownerName != null) {
                if (ownerName.startsWith("uid:")) // set uid if specified in ACL
                    Files.setAttribute(file.toPath(), "unix:uid", Integer.parseInt(ownerName.substring(4)), getLinkOptions());
                else // otherwise set owner by name (look up principals first)
                    attributeView.setOwner(lookupService.lookupPrincipalByName(ownerName));
            }

            if (groupOwnerName != null) {
                if (groupOwnerName.startsWith("gid:")) // set gid if specified in ACL
                    Files.setAttribute(file.toPath(), "unix:gid", Integer.parseInt(groupOwnerName.substring(4)), getLinkOptions());
                else // otherwise set group owner by name (look up principals first)
                    attributeView.setGroup(lookupService.lookupPrincipalByGroupName(groupOwnerName));
            }
        } catch (IOException e) {
            throw new RuntimeException("could not write file attributes for " + file.getPath(), e);
        }
    }

    private synchronized void mkdirs(File dir) {
        try {
            Files.createDirectories(dir.toPath());
        } catch (IOException e) {
            throw new RuntimeException("failed to create directory " + dir, e);
        }
    }

    private synchronized void mkdir(File dir) throws IOException {
        Path path = dir.toPath();
        if (Files.exists(path)) {
            if (!Files.isDirectory(path)) throw new RuntimeException("path exists and is a file");
        } else {
            Files.createDirectory(path);
        }
    }

    private void copyData(InputStream inStream, File outFile) throws IOException {
        byte[] buffer = new byte[options.getBufferSize()];
        int c;
        try (InputStream input = inStream; OutputStream output = createOutputStream(outFile)) {
            while ((c = input.read(buffer)) != -1) {
                output.write(buffer, 0, c);
                if (options.isMonitorPerformance()) getWriteWindow().increment(c);
            }
        }
    }

    @Override
    public void delete(String identifier) {
        File deleteCheckScript = null;
        if (config.getDeleteCheckScript() != null)
            deleteCheckScript = new File(config.getDeleteCheckScript());
        delete(identifier, config.getDeleteOlderThan(), deleteCheckScript);
    }

    public void delete(String identifier, long deleteOlderThan, File deleteCheckScript) {
        File objectFile = createFile(identifier);
        File metaFile = getMetaFile(objectFile);
        if (metaFile.exists()) delete(metaFile, deleteOlderThan, deleteCheckScript);
        delete(objectFile, deleteOlderThan, deleteCheckScript);
    }

    protected void delete(File file, long deleteOlderThan, File deleteCheckScript) {
        if (file.isDirectory()) {
            synchronized (this) {
                File metaDir = getMetaFile(file).getParentFile();
                if (metaDir.exists() && !metaDir.delete())
                    log.warn("failed to delete metaDir {}", metaDir);
                // Just try and delete dir
                if (!file.delete()) {
                    log.warn("failed to delete directory {}", file);
                }
            }
        } else {
            // Must make sure to throw exceptions when necessary to flag actual failures as opposed to skipped files.
            boolean tryDelete = true;
            if (deleteOlderThan > 0) {
                if (System.currentTimeMillis() - file.lastModified() < deleteOlderThan) {
                    log.info("not deleting {}; it is not at least {} ms old", file, deleteOlderThan);
                    tryDelete = false;
                }
            }
            if (deleteCheckScript != null) {
                String[] args = new String[]{
                        deleteCheckScript.getAbsolutePath(),
                        file.getAbsolutePath()
                };
                try {
                    log.debug("delete check: " + Arrays.asList(args));
                    Process p = Runtime.getRuntime().exec(args);
                    while (true) {
                        try {
                            int exitCode = p.exitValue();

                            if (exitCode == 0) {
                                log.debug("delete check OK, exit code {}", exitCode);
                            } else {
                                log.info("delete check failed, exit code {}.  Not deleting file.", exitCode);
                                tryDelete = false;
                            }
                            break;
                        } catch (IllegalThreadStateException e) {
                            // Ignore.
                        }
                    }
                } catch (IOException e) {
                    log.info("error executing delete check script: {}.  Not deleting file.", e.toString());
                    tryDelete = false;
                }
            }
            if (tryDelete) {
                log.debug("deleting {}", file);

                // Try to lock the file first.  If this fails, the file is
                // probably open for write somewhere.
                // Note that on a mac, you can apparently delete files that
                // someone else has open for writing, and can lock files
                // too.
                try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                    FileChannel fc = raf.getChannel();
                    FileLock flock = fc.lock();
                    // If we got here, we should be good.
                    flock.release();
                    if (!file.delete()) {
                        throw new RuntimeException(MessageFormat.format("failed to delete {0}", file));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(MessageFormat.format("file {0} not deleted, it appears to be open: {1}",
                            file, e.getMessage()));
                }
            }
        }
    }

    private File getMetaFile(File objectFile) {
        return createFile(ObjectMetadata.getMetaPath(objectFile.getPath(), objectFile.isDirectory()));
    }

    private boolean isSymLink(File file) {
        return Files.isSymbolicLink(file.toPath());
    }

    private String getLinkTarget(File file) {
        try {
            String target = Files.readSymbolicLink(file.toPath()).toString();

            // translate to relative path if target is under our source location
            if (config.isRelativeLinkTargets() && target.startsWith(config.getPath())) {
                Path sourcePath = file.toPath();
                Path targetPath = Paths.get(target);
                target = sourcePath.getParent().relativize(targetPath).toString();
            }
            return target;
        } catch (IOException e) {
            throw new RuntimeException("could not read link target for " + file.getPath(), e);
        }
    }

    private Set<PosixFilePermission> getPosixPermissions(Collection<String> permissions, PosixType type) {
        Set<PosixFilePermission> posixPermissions = new HashSet<>();
        for (String permission : permissions) {
            if (READ.equals(permission)) {
                if (PosixType.OWNER == type) posixPermissions.add(PosixFilePermission.OWNER_READ);
                else if (PosixType.GROUP == type) posixPermissions.add(PosixFilePermission.GROUP_READ);
                else if (PosixType.OTHER == type) posixPermissions.add(PosixFilePermission.OTHERS_READ);
            } else if (WRITE.equals(permission)) {
                if (PosixType.OWNER == type) posixPermissions.add(PosixFilePermission.OWNER_WRITE);
                else if (PosixType.GROUP == type) posixPermissions.add(PosixFilePermission.GROUP_WRITE);
                else if (PosixType.OTHER == type) posixPermissions.add(PosixFilePermission.OTHERS_WRITE);
            } else if (EXECUTE.equals(permission)) {
                if (PosixType.OWNER == type) posixPermissions.add(PosixFilePermission.OWNER_EXECUTE);
                else if (PosixType.GROUP == type) posixPermissions.add(PosixFilePermission.GROUP_EXECUTE);
                else if (PosixType.OTHER == type) posixPermissions.add(PosixFilePermission.OTHERS_EXECUTE);
            } else {
                log.warn("{} does not map to a POSIX permission (you should use the ACL mapper)", permission);
            }
        }
        return posixPermissions;
    }

    private String fromPosixPermission(PosixFilePermission permission) {
        switch (permission) {
            case OWNER_READ:
            case GROUP_READ:
            case OTHERS_READ:
                return READ;
            case OWNER_WRITE:
            case GROUP_WRITE:
            case OTHERS_WRITE:
                return WRITE;
            case OWNER_EXECUTE:
            case GROUP_EXECUTE:
            case OTHERS_EXECUTE:
                return EXECUTE;
            default:
                throw new IllegalArgumentException("unknown POSIX permission: " + permission);
        }
    }

    private BasicFileAttributes readAttributes(File file) {
        try {
            return Files.readAttributes(file.toPath(), PosixFileAttributes.class, getLinkOptions());
        } catch (Exception e) {
            log.info("could not get POSIX file attributes for {}: {}", file.getPath(), e);
            try {
                return Files.readAttributes(file.toPath(), BasicFileAttributes.class, getLinkOptions());
            } catch (Exception e2) {
                throw new RuntimeException("could not get BASIC file attributes for " + file, e2);
            }
        }
    }

    public DirectoryStream.Filter<Path> getFilter() {
        return filter;
    }

    protected LinkOption[] getLinkOptions() {
        return config.isFollowLinks() ? new LinkOption[0] : new LinkOption[]{LinkOption.NOFOLLOW_LINKS};
    }

    private class SourceFilter implements DirectoryStream.Filter<Path> {
        @Override
        public boolean accept(Path path) {
            String name = path.getFileName().toString();
            if (ObjectMetadata.METADATA_DIR.equals(name) || ObjectMetadata.DIR_META_FILE.equals(name)) return false;

            File target = createFile(path.toString());

            // modified since filter
            try {
                if (modifiedSince != null) {
                    if (!Files.isDirectory(target.toPath(), getLinkOptions())) {
                        long mtime = Files.getLastModifiedTime(target.toPath(), getLinkOptions()).toMillis();
                        if (mtime <= modifiedSince.getTime()) return false;
                    }
                }
            } catch (IOException e) {
                log.warn("could not read last-modified time for " + target.getPath(), e);
            }

            // exclude paths filter
            if (excludedPathPatterns != null) {
                for (Pattern p : excludedPathPatterns) {
                    if (p.matcher(target.getPath()).matches()) {
                        if (log.isDebugEnabled()) log.debug("skipping file {}: matches pattern: {}", target, p);
                        return false;
                    }
                }
            }

            return true;
        }
    }

    private enum PosixType {
        OWNER, GROUP, OTHER
    }
}

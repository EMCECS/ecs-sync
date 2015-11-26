/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.util;

import com.emc.ecs.sync.model.SyncAcl;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.SyncObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.*;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public final class FilesystemUtil {
    private static final Logger log = LoggerFactory.getLogger(FilesystemUtil.class);

    public static final String OTHER_GROUP = "other";

    public static final String READ = "READ";
    public static final String WRITE = "WRITE";
    public static final String EXECUTE = "EXECUTE";

    public static final String TYPE_LINK = "application/x-symlink";
    public static final String META_LINK_TARGET = "x-emc-link-target";
    public static final String META_MTIME = "x-emc-mtime";
    public static final String META_ATIME = "x-emc-atime";
    public static final String META_CRTIME = "x-emc-crtime";

    public static boolean isSymLink(File file) {
        return Files.isSymbolicLink(file.toPath());
    }

    public static boolean isSymLink(SyncObject object) {
        SyncMetadata metadata = object.getMetadata();
        return metadata != null && TYPE_LINK.equals(metadata.getContentType());
    }

    public static String getLinkTarget(File file) {
        try {
            return Files.readSymbolicLink(file.toPath()).toString();
        } catch (IOException e) {
            throw new RuntimeException("could not read link target for " + file.getPath(), e);
        }
    }

    public static String getLinkTarget(SyncObject object) {
        SyncMetadata metadata = object.getMetadata();
        if (metadata != null && metadata.getUserMetadata() != null)
            return metadata.getUserMetadataValue(META_LINK_TARGET);
        return null;
    }

    public static SyncMetadata createSymLinkMetadata(File file, boolean includeAcl) {
        SyncMetadata metadata = new SyncMetadata();
        setTimeMeta(file, metadata, false);
        metadata.setContentType(TYPE_LINK);
        metadata.setContentLength(0);
        metadata.setUserMetadataValue(META_LINK_TARGET, getLinkTarget(file));

        if (includeAcl) metadata.setAcl(createPosixAcl(file, false));

        return metadata;
    }

    public static SyncMetadata createFilesystemMetadata(File file, MimetypesFileTypeMap mimeMap, boolean includeAcl) {
        SyncMetadata metadata = new SyncMetadata();
        setTimeMeta(file, metadata, true);
        metadata.setContentType(mimeMap.getContentType(file));
        // On OSX, directories have 'length'... ignore.
        if(file.isFile()) {
            metadata.setContentLength(file.length());
        }

        if (includeAcl) metadata.setAcl(createPosixAcl(file, true));

        return metadata;
    }

    public static Date getMtime(SyncObject object) {
        SyncMetadata metadata = object.getMetadata();

        if (metadata.getUserMetadataValue(META_MTIME) != null)
            return Iso8601Util.parse(metadata.getUserMetadataValue(META_MTIME));

        else return metadata.getModificationTime();
    }

    public static void setTimeMeta(File file, SyncMetadata metadata, boolean followLinks) {
        FileTime mtime, atime, crtime;
        try {
            BasicFileAttributes attributes;
            if (followLinks) {
                attributes = Files.getFileAttributeView(file.toPath(), BasicFileAttributeView.class).readAttributes();
            } else {
                attributes = Files.getFileAttributeView(file.toPath(), BasicFileAttributeView.class,
                        LinkOption.NOFOLLOW_LINKS).readAttributes();
            }
            mtime = attributes.lastModifiedTime();
            atime = attributes.lastAccessTime();
            crtime = attributes.creationTime();
        } catch (IOException e) {
            throw new RuntimeException("could not get file times for link " + file.getPath(), e);
        }

        metadata.setModificationTime(new Date(mtime.toMillis()));
        metadata.setUserMetadataValue(META_MTIME, mtime.toString());
        if (atime != null) metadata.setUserMetadataValue(META_ATIME, atime.toString());
        if (crtime != null) metadata.setUserMetadataValue(META_CRTIME, crtime.toString());
    }

    public static SyncAcl createPosixAcl(File file, boolean followLinks) {
        SyncAcl acl = new SyncAcl();

        // build POSIX ACL
        PosixFileAttributes attributes;
        try {
            if (followLinks) attributes = Files.readAttributes(file.toPath(), PosixFileAttributes.class);
            else attributes = Files.readAttributes(file.toPath(), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException e) {
            throw new RuntimeException("could not read file attributes for " + file.getPath(), e);
        }

        if (attributes.owner() != null) acl.setOwner(attributes.owner().getName());

        String group = null;
        if (attributes.group() != null) group = attributes.group().getName();

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

    public static void applyFilesystemMetadata(File file, SyncMetadata metadata, boolean includeAcl) {
        if (includeAcl && metadata.getAcl() != null) {
            SyncAcl acl = metadata.getAcl();
            Set<PosixFilePermission> permissions = new HashSet<>();

            // extract the group owner. since SyncAcl does not provide the group owner directly, take the first group in
            // the grant list that's not "other"
            String group = null;
            for (String groupName : acl.getGroupGrants().keySet()) {
                if (groupName.equals(FilesystemUtil.OTHER_GROUP)) {
                    // add all "other" permissions
                    permissions.addAll(getPosixPermissions(acl.getGroupGrants().get(groupName), PosixType.OTHER));
                } else if (group == null) {
                    group = groupName;
                    // add group owner permissions
                    permissions.addAll(getPosixPermissions(acl.getGroupGrants().get(groupName), PosixType.GROUP));
                }
            }

            String user = acl.getOwner();
            for (String userName : acl.getUserGrants().keySet()) {
                if (user == null) user = userName;
                if (user.equals(userName)) {
                    // add owner permissions
                    permissions.addAll(getPosixPermissions(acl.getUserGrants().get(userName), PosixType.OWNER));
                }
            }

            try {
                Path path = file.toPath();
                // lookup user and group principals
                UserPrincipal owner = path.getFileSystem().getUserPrincipalLookupService().lookupPrincipalByName(user);
                GroupPrincipal groupOwner = path.getFileSystem().getUserPrincipalLookupService().lookupPrincipalByGroupName(group);
                // set owner/group-owner
                Files.setOwner(path, owner);
                Files.getFileAttributeView(path, PosixFileAttributeView.class).setGroup(groupOwner);
                // set permission bits
                Files.setPosixFilePermissions(path, permissions);
            } catch (IOException e) {
                throw new RuntimeException("could not write file attributes for " + file.getPath(), e);
            }
        }

        // set file times
        // Note: directory times may be overwritten if any children are modified
        FileTime mtime = null, atime = null, crtime = null;
        if (metadata.getModificationTime() != null)
            mtime = FileTime.fromMillis(metadata.getModificationTime().getTime());
        if (metadata.getUserMetadataValue(META_MTIME) != null) {
            Date date = Iso8601Util.parse(metadata.getUserMetadataValue(META_MTIME));
            if (date != null) mtime = FileTime.fromMillis(date.getTime());
        }
        if (metadata.getUserMetadataValue(META_ATIME) != null) {
            Date date = Iso8601Util.parse(metadata.getUserMetadataValue(META_ATIME));
            if (date != null) atime = FileTime.fromMillis(date.getTime());
        }
        if (metadata.getUserMetadataValue(META_CRTIME) != null) {
            Date date = Iso8601Util.parse(metadata.getUserMetadataValue(META_CRTIME));
            if (date != null) crtime = FileTime.fromMillis(date.getTime());
        }
        try {
            BasicFileAttributeView view = Files.getFileAttributeView(file.toPath(), BasicFileAttributeView.class,
                    LinkOption.NOFOLLOW_LINKS);
            view.setTimes(mtime, atime, crtime);
        } catch (Throwable t) {
            log.warn("could not set file times for {}: {}", file.getPath(), t);
        }
    }

    private static Set<PosixFilePermission> getPosixPermissions(Collection<String> permissions, PosixType type) {
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

    private static String fromPosixPermission(PosixFilePermission permission) {
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

    private enum PosixType {
        OWNER, GROUP, OTHER
    }

    private FilesystemUtil() {
    }
}

/*
 * Copyright (c) 2016-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.filter.PreserveAclConfig;
import com.emc.ecs.sync.config.filter.PreserveFileAttributesConfig;
import com.emc.ecs.sync.config.filter.RestoreAclConfig;
import com.emc.ecs.sync.config.filter.RestoreFileAttributesConfig;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.file.AbstractFilesystemStorage;
import com.emc.ecs.sync.util.Iso8601Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.*;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public final class PreserveFilters {
    private static final Logger log = LoggerFactory.getLogger(PreserveFilters.class);

    /**
     * Old ecs-sync archive metadata (need to read this for compatibility):
     * x-emc-mtime: <ISO-8601-timestamp> (to seconds)
     * x-emc-atime: <ISO-8601-timestamp> (to seconds)
     * x-emc-crtime: <ISO-8601-timestamp> (to seconds) (**creation-time)
     * x-emc-posix-mode: <rwx-perm-string>
     * x-emc-posix-owner-name: <username> (if available)
     * x-emc-posix-group-owner-name: <groupname> (if available)
     * x-emc-posix-uid: <uid> (if available)
     * x-emc-posix-gid: <gid> (if available)
     */
    public static final String OLD_META_MTIME = "x-emc-mtime";
    public static final String OLD_META_ATIME = "x-emc-atime";
    public static final String OLD_META_CRTIME = "x-emc-crtime";
    public static final String OLD_META_POSIX_MODE = "x-emc-posix-mode";
    public static final String OLD_META_POSIX_OWNER = "x-emc-posix-owner-name";
    public static final String OLD_META_POSIX_GROUP_OWNER = "x-emc-posix-group-owner-name";
    public static final String OLD_META_POSIX_UID = "x-emc-posix-uid";
    public static final String OLD_META_POSIX_GID = "x-emc-posix-gid";

    /**
     * current archive metadata (consistent with other Dell EMC data movers):
     * file-owner: <uid> if available, otherwise owner user name
     * file-group: <gid> if available, otherwise owner group name
     * file-atime: <epoch-seconds.hundredths>
     * file-mtime: <epoch-seconds.hundredths>
     * file-ctime: <epoch-seconds.hundredths>
     * file-crtime: <epoch-seconds.hundredths> (**creation-time)
     * file-permissions: <4-digit-octal>
     */
    public static final String META_OWNER = "file-owner";
    public static final String META_GROUP = "file-group";
    public static final String META_ATIME = "file-atime";
    public static final String META_MTIME = "file-mtime";
    public static final String META_CTIME = "file-ctime";
    public static final String META_CRTIME = "file-crtime";
    public static final String META_PERMISSIONS = "file-permissions";

    public static final String META_ACL_JSON = "x-emc-preserved-acl";

    private PreserveFilters() {
    }

    public static class PreserveAclFilter extends AbstractFilter<PreserveAclConfig> {
        @Override
        public void filter(ObjectContext objectContext) {
            SyncObject object = objectContext.getObject();

            // preserve ACL as user metadata
            if (object.getAcl() != null)
                object.getMetadata().setUserMetadataValue(META_ACL_JSON, object.getAcl().toJson());

            getNext().filter(objectContext);
        }

        @Override
        public SyncObject reverseFilter(ObjectContext objectContext) {
            SyncObject object = getNext().reverseFilter(objectContext);

            // remove preserved ACL from user metadata
            object.getMetadata().getUserMetadata().remove(META_ACL_JSON);

            return object;
        }
    }

    public static class RestoreAclFilter extends AbstractFilter<RestoreAclConfig> {
        @Override
        public void filter(ObjectContext objectContext) {
            SyncObject object = objectContext.getObject();

            // restore preserved ACL to object
            String json = object.getMetadata().getUserMetadataValue(META_ACL_JSON);
            if (json != null) object.setAcl(ObjectAcl.fromJson(json));

            // remove preserved ACL from user metadata
            object.getMetadata().getUserMetadata().remove(META_ACL_JSON);

            getNext().filter(objectContext);
        }

        // TODO: if verification ever includes ACLs, need to display a warning that this filter cannot be reversed
        @Override
        public SyncObject reverseFilter(ObjectContext objectContext) {
            return getNext().reverseFilter(objectContext);
        }
    }

    public static class PreserveFileAttributesFilter extends AbstractFilter<PreserveFileAttributesConfig> {
        private static final Logger log = LoggerFactory.getLogger(PreserveFileAttributesFilter.class);

        @Override
        public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
            super.configure(source, filters, target);

            // check that the source is a Filesystem
            if (!AbstractFilesystemStorage.class.isAssignableFrom(source.getClass()))
                throw new ConfigurationException("You can only preserve file attributes from a filesystem source");
        }

        @Override
        public void filter(ObjectContext objectContext) {
            File file = (File) objectContext.getObject().getProperty(AbstractFilesystemStorage.PROP_FILE);

            if (file == null) throw new RuntimeException("could not get source file");

            ObjectMetadata metadata = objectContext.getObject().getMetadata();
            boolean link = AbstractFilesystemStorage.TYPE_LINK.equals(metadata.getContentType());

            BasicFileAttributes basicAttr = readAttributes(file, link);
            PosixFileAttributes posixAttr = null;
            if (basicAttr instanceof PosixFileAttributes) posixAttr = (PosixFileAttributes) basicAttr;

            long mtime = basicAttr.lastModifiedTime().toMillis();
            long atime = basicAttr.lastAccessTime() != null ? basicAttr.lastAccessTime().toMillis() : 0;
            long crtime = basicAttr.creationTime() != null ? basicAttr.creationTime().toMillis() : 0;

            // preserve file times
            metadata.setUserMetadataValue(META_MTIME, String.format("%d.%02d", mtime / 1000, (mtime % 1000) / 10));
            if (atime != 0)
                metadata.setUserMetadataValue(META_ATIME, String.format("%d.%02d", atime / 1000, (atime % 1000) / 10));
            if (crtime != 0)
                metadata.setUserMetadataValue(META_CRTIME, String.format("%d.%02d", crtime / 1000, (crtime % 1000) / 10));

            // preserve POSIX ACL
            if (posixAttr != null) {
                if (posixAttr.owner() != null)
                    metadata.setUserMetadataValue(OLD_META_POSIX_OWNER, posixAttr.owner().getName());
                if (posixAttr.group() != null)
                    metadata.setUserMetadataValue(OLD_META_POSIX_GROUP_OWNER, posixAttr.group().getName());
                // NOTE: this won't get sticky bit, etc. (we try to get that below)
                metadata.setUserMetadataValue(META_PERMISSIONS, getOctalMode(posixAttr.permissions()));

                // *try* to get uid/gid/ctime/mode too (this will override owner/group-owner/permissions)
                try {
                    LinkOption[] options = link ? new LinkOption[]{LinkOption.NOFOLLOW_LINKS} : new LinkOption[0];
                    Map<String, Object> attrs = Files.readAttributes(file.toPath(), "unix:uid,gid,ctime,mode", options);
                    Integer uid = (Integer) attrs.get("uid");
                    Integer gid = (Integer) attrs.get("gid");
                    long ctime = attrs.get("ctime") != null ? ((FileTime) attrs.get("ctime")).toMillis() : 0;
                    Integer mode = (Integer) attrs.get("mode");
                    if (uid != null) metadata.setUserMetadataValue(META_OWNER, uid.toString());
                    if (gid != null) metadata.setUserMetadataValue(META_GROUP, gid.toString());
                    if (ctime != 0)
                        metadata.setUserMetadataValue(META_CTIME, String.format("%d.%02d", ctime / 1000, (ctime % 1000) / 10));
                    // only want last 4 octals (not sure why there are more in some cases)
                    if (mode != null)
                        metadata.setUserMetadataValue(META_PERMISSIONS, String.format("%04o", mode & 07777));
                } catch (IOException e) {
                    log.warn("could not get uid/gid/ctime/mode", e);
                }
            }

            getNext().filter(objectContext);
        }

        @Override
        public SyncObject reverseFilter(ObjectContext objectContext) {
            SyncObject object = getNext().reverseFilter(objectContext);

            removeMetadata(object);

            return object;
        }

        // remove all preserved metadata
        static void removeMetadata(SyncObject object) {
            for (String key : new String[]{META_OWNER, META_GROUP, META_MTIME, META_ATIME, META_CTIME,
                    META_CRTIME, META_PERMISSIONS}) {
                object.getMetadata().getUserMetadata().remove(key);
            }
        }

        static BasicFileAttributes readAttributes(File file, boolean link) {
            try {
                if (link)
                    return Files.readAttributes(file.toPath(), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                else return Files.readAttributes(file.toPath(), PosixFileAttributes.class);
            } catch (Exception e) {
                log.info("could not get POSIX file attributes for {}: {}", file.getPath(), e);
                try {
                    if (link)
                        return Files.readAttributes(file.toPath(), BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    else return Files.readAttributes(file.toPath(), BasicFileAttributes.class);
                } catch (Exception e2) {
                    throw new RuntimeException("could not get BASIC file attributes for " + file, e2);
                }
            }
        }
    }

    public static class RestoreFileAttributesFilter extends AbstractFilter<RestoreFileAttributesConfig> {
        AbstractFilesystemStorage target;

        @Override
        public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
            super.configure(source, filters, target);

            // check that the target is a Filesystem
            if (!AbstractFilesystemStorage.class.isAssignableFrom(target.getClass()))
                throw new ConfigurationException("You can only restore file attributes with a filesystem target");

            this.target = (AbstractFilesystemStorage) target;
        }

        @Override
        public void filter(ObjectContext objectContext) {
            getNext().filter(objectContext);

            File file = target.createFile(objectContext.getTargetId());
            if (file == null) throw new RuntimeException("could not get target file");
            Path path = file.toPath();

            ObjectMetadata metadata = objectContext.getObject().getMetadata();
            boolean link = AbstractFilesystemStorage.TYPE_LINK.equals(metadata.getContentType());

            // set file times
            // Note: directory times may be overwritten if any children are modified
            if (!link) { // cannot set times for symlinks in Java
                setFileTimesCompat(path, metadata);
                setFileTimes(path, metadata);
            }

            // uid/gid
            String uid = metadata.getUserMetadataValue(META_OWNER);
            String gid = metadata.getUserMetadataValue(META_GROUP);

            // mode
            String mode = metadata.getUserMetadataValue(META_PERMISSIONS);

            // owner/group names (legacy)
            String ownerName = metadata.getUserMetadataValue(OLD_META_POSIX_OWNER);
            String groupOwnerName = metadata.getUserMetadataValue(OLD_META_POSIX_GROUP_OWNER);

            // permissions (legacy)
            Set<PosixFilePermission> permissions = null;
            if (metadata.getUserMetadataValue(OLD_META_POSIX_MODE) != null)
                permissions = PosixFilePermissions.fromString(metadata.getUserMetadataValue(OLD_META_POSIX_MODE));

            LinkOption[] linkOptions = link ? new LinkOption[]{LinkOption.NOFOLLOW_LINKS} : new LinkOption[0];

            try {
                PosixFileAttributeView attributeView = Files.getFileAttributeView(path, PosixFileAttributeView.class, linkOptions);

                // set permission bits
                // cannot set mode of symlinks in Java
                if (mode != null && !link) {
                    log.debug("setting mode of {} to {}", path.toString(), mode);
                    Files.setAttribute(path, "unix:mode", Integer.parseInt(mode, 8));
                } else if (permissions != null && !link) {
                    log.debug("setting permissions of {} to {}", path.toString(), metadata.getUserMetadataValue(OLD_META_POSIX_MODE));
                    attributeView.setPermissions(permissions);
                }

                // set ownership
                // uid/gid takes priority
                if (uid != null) {
                    log.debug("setting ownership of {} to {}.{}", path.toString(), uid, gid);
                    Files.setAttribute(path, "unix:uid", Integer.parseInt(uid), linkOptions);
                    Files.setAttribute(path, "unix:gid", Integer.parseInt(gid), linkOptions);
                } else {
                    UserPrincipalLookupService lookupService = path.getFileSystem().getUserPrincipalLookupService();

                    // set owner/group-owner (look up principals first)
                    if (ownerName != null) {
                        log.debug("setting ownership of {} to {}", path.toString(), ownerName);
                        attributeView.setOwner(lookupService.lookupPrincipalByName(ownerName));
                    }
                    if (groupOwnerName != null) {
                        log.debug("setting group-ownership of {} to {}", path.toString(), groupOwnerName);
                        attributeView.setGroup(lookupService.lookupPrincipalByGroupName(groupOwnerName));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("could not write file attributes for " + path.toString(), e);
            }

            PreserveFileAttributesFilter.removeMetadata(objectContext.getObject());
            removeLegacyMetadata(objectContext.getObject());
        }

        // TODO: if verification ever includes ACLs, need to display a warning that this filter cannot be reversed
        @Override
        public SyncObject reverseFilter(ObjectContext objectContext) {
            return getNext().reverseFilter(objectContext);
        }

        // NOTE: unfortunately you cannot set ctime (unix change time) in Java
        private void setFileTimes(Path path, ObjectMetadata metadata) {
            FileTime mtime = parseFileTimeFromEpoch(path, META_MTIME, metadata.getUserMetadataValue(META_MTIME));
            FileTime atime = parseFileTimeFromEpoch(path, META_ATIME, metadata.getUserMetadataValue(META_ATIME));
            FileTime crtime = parseFileTimeFromEpoch(path, META_CRTIME, metadata.getUserMetadataValue(META_CRTIME));

            setBasicFileTimes(path, mtime, atime, crtime);
        }

        private FileTime parseFileTimeFromEpoch(Path path, String fieldName, String epochFloatSeconds) {
            if (epochFloatSeconds == null) return null;
            try {
                return FileTime.fromMillis((long) (Double.parseDouble(epochFloatSeconds) * 1000D));
            } catch (Throwable t) {
                if (config.isFailOnParseError()) {
                    // let parse errors bubble up and cause the object to fail (value may be corrupt)
                    throw new RuntimeException(String.format("Could not parse %s for %s from value %s",
                            fieldName, path.toString(), epochFloatSeconds), t);
                } else {
                    log.warn("could not parse {} for {} from value {}", fieldName, path, epochFloatSeconds);
                    return null;
                }
            }
        }

        // for compatibility with archives from previous versions
        private void setFileTimesCompat(Path path, ObjectMetadata metadata) {
            FileTime mtime = null, atime = null, crtime = null;
            if (metadata.getModificationTime() != null)
                mtime = FileTime.fromMillis(metadata.getModificationTime().getTime());
            if (metadata.getUserMetadataValue(OLD_META_MTIME) != null) {
                Date date = Iso8601Util.parse(metadata.getUserMetadataValue(OLD_META_MTIME));
                if (date != null) mtime = FileTime.fromMillis(date.getTime());
            }
            if (metadata.getAccessTime() != null)
                atime = FileTime.fromMillis(metadata.getAccessTime().getTime());
            if (metadata.getUserMetadataValue(OLD_META_ATIME) != null) {
                Date date = Iso8601Util.parse(metadata.getUserMetadataValue(OLD_META_ATIME));
                if (date != null) atime = FileTime.fromMillis(date.getTime());
            }
            if (metadata.getUserMetadataValue(OLD_META_CRTIME) != null) {
                Date date = Iso8601Util.parse(metadata.getUserMetadataValue(OLD_META_CRTIME));
                if (date != null) crtime = FileTime.fromMillis(date.getTime());
            }
            setBasicFileTimes(path, mtime, atime, crtime);
        }

        private void setBasicFileTimes(Path path, FileTime mtime, FileTime atime, FileTime crtime) {
            if (mtime != null || atime != null || crtime != null) {
                try {
                    log.debug("setting times of {} to mtime:{} atime:{} crtime:{}", path.toString(), mtime, atime, crtime);
                    BasicFileAttributeView view = Files.getFileAttributeView(path, BasicFileAttributeView.class,
                            LinkOption.NOFOLLOW_LINKS);
                    view.setTimes(mtime, atime, crtime);
                } catch (Throwable t) {
                    throw new RuntimeException("could not set file times for " + path.toString(), t);
                }
            }
        }

        private void removeLegacyMetadata(SyncObject object) {
            for (String key : new String[]{OLD_META_MTIME, OLD_META_ATIME, OLD_META_CRTIME, OLD_META_POSIX_OWNER,
                    OLD_META_POSIX_GROUP_OWNER, OLD_META_POSIX_UID, OLD_META_POSIX_GID, OLD_META_POSIX_MODE}) {
                object.getMetadata().getUserMetadata().remove(key);
            }
        }
    }

    static String getOctalMode(Set<PosixFilePermission> permissions) {
        int mode = 0;
        for (PosixFilePermission permission : permissions) {
            switch (permission) {
                case OWNER_READ:
                    mode |= 0b100000000;
                    break;
                case OWNER_WRITE:
                    mode |= 0b010000000;
                    break;
                case OWNER_EXECUTE:
                    mode |= 0b001000000;
                    break;
                case GROUP_READ:
                    mode |= 0b000100000;
                    break;
                case GROUP_WRITE:
                    mode |= 0b000010000;
                    break;
                case GROUP_EXECUTE:
                    mode |= 0b000001000;
                    break;
                case OTHERS_READ:
                    mode |= 0b000000100;
                    break;
                case OTHERS_WRITE:
                    mode |= 0b000000010;
                    break;
                case OTHERS_EXECUTE:
                    mode |= 0b000000001;
                    break;
            }
        }
        return String.format("%04o", mode);
    }
}

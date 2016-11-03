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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.filter.PreserveAclConfig;
import com.emc.ecs.sync.config.filter.PreserveFileAttributesConfig;
import com.emc.ecs.sync.config.filter.RestoreAclConfig;
import com.emc.ecs.sync.config.filter.RestoreFileAttributesConfig;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.AbstractFilesystemStorage;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.util.Iso8601Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.*;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

public final class PreserveFilters {
    public static final String META_MTIME = "x-emc-mtime";
    public static final String META_ATIME = "x-emc-atime";
    public static final String META_CRTIME = "x-emc-crtime";
    public static final String META_POSIX_MODE = "x-emc-posix-mode";
    public static final String META_POSIX_OWNER = "x-emc-posix-owner-name";
    public static final String META_POSIX_GROUP_OWNER = "x-emc-posix-group-owner-name";
    public static final String META_POSIX_UID = "x-emc-posix-uid";
    public static final String META_POSIX_GID = "x-emc-posix-gid";

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
        public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
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

            FileTime mtime = basicAttr.lastModifiedTime();
            FileTime atime = basicAttr.lastAccessTime();
            FileTime crtime = basicAttr.creationTime();

            // preserve file times
            metadata.setUserMetadataValue(META_MTIME, mtime.toString());
            if (atime != null) metadata.setUserMetadataValue(META_ATIME, atime.toString());
            if (crtime != null) metadata.setUserMetadataValue(META_CRTIME, crtime.toString());

            // preserve POSIX ACL
            if (posixAttr != null) {
                if (posixAttr.owner() != null)
                    metadata.setUserMetadataValue(META_POSIX_OWNER, posixAttr.owner().getName());
                if (posixAttr.group() != null)
                    metadata.setUserMetadataValue(META_POSIX_GROUP_OWNER, posixAttr.group().getName());
                metadata.setUserMetadataValue(META_POSIX_MODE, PosixFilePermissions.toString(posixAttr.permissions()));

                // *try* to get uid/gid too (this will override owner/group-owner)
                try {
                    LinkOption[] options = link ? new LinkOption[]{LinkOption.NOFOLLOW_LINKS} : new LinkOption[0];
                    Integer uid = (Integer) Files.getAttribute(file.toPath(), "unix:uid", options);
                    Integer gid = (Integer) Files.getAttribute(file.toPath(), "unix:gid", options);
                    if (uid != null) metadata.setUserMetadataValue(META_POSIX_UID, uid.toString());
                    if (gid != null) metadata.setUserMetadataValue(META_POSIX_GID, gid.toString());
                } catch (IOException e) {
                    log.warn("could not get uid/gid", e);
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
            for (String key : new String[]{META_MTIME, META_ATIME, META_CRTIME, META_POSIX_MODE,
                    META_POSIX_OWNER, META_POSIX_GROUP_OWNER, META_POSIX_UID, META_POSIX_GID}) {
                object.getMetadata().getUserMetadata().remove(key);
            }
        }

        private BasicFileAttributes readAttributes(File file, boolean link) {
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
        @Override
        public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
            super.configure(source, filters, target);

            // check that the source is a Filesystem
            if (!AbstractFilesystemStorage.class.isAssignableFrom(target.getClass()))
                throw new ConfigurationException("You can only restore file attributes with a filesystem target");
        }

        @Override
        public void filter(ObjectContext objectContext) {
            getNext().filter(objectContext);

            File file = (File) objectContext.getObject().getProperty(AbstractFilesystemStorage.PROP_FILE);

            if (file == null) throw new RuntimeException("could not get target file");

            ObjectMetadata metadata = objectContext.getObject().getMetadata();
            boolean link = AbstractFilesystemStorage.TYPE_LINK.equals(metadata.getContentType());

            String ownerName = null;
            String groupOwnerName = null;
            Set<PosixFilePermission> permissions = null;

            if (metadata.getUserMetadataValue(META_POSIX_OWNER) != null)
                ownerName = metadata.getUserMetadataValue(META_POSIX_OWNER);
            if (metadata.getUserMetadataValue(META_POSIX_GROUP_OWNER) != null)
                groupOwnerName = metadata.getUserMetadataValue(META_POSIX_GROUP_OWNER);
            if (metadata.getUserMetadataValue(META_POSIX_MODE) != null)
                permissions = PosixFilePermissions.fromString(metadata.getUserMetadataValue(META_POSIX_MODE));

            LinkOption[] linkOptions = link ? new LinkOption[]{LinkOption.NOFOLLOW_LINKS} : new LinkOption[0];

            try {
                PosixFileAttributeView attributeView = Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class, linkOptions);

                // uid/gid takes priority
                String uid = metadata.getUserMetadataValue(META_POSIX_UID);
                String gid = metadata.getUserMetadataValue(META_POSIX_GID);
                if (uid != null) {
                    Files.setAttribute(file.toPath(), "unix:uid", Integer.parseInt(uid), linkOptions);
                    Files.setAttribute(file.toPath(), "unix:gid", Integer.parseInt(gid), linkOptions);
                } else {
                    UserPrincipalLookupService lookupService = file.toPath().getFileSystem().getUserPrincipalLookupService();

                    // set owner/group-owner (look up principals first)
                    if (ownerName != null) attributeView.setOwner(lookupService.lookupPrincipalByName(ownerName));
                    if (groupOwnerName != null)
                        attributeView.setGroup(lookupService.lookupPrincipalByGroupName(groupOwnerName));
                }

                // set permission bits
                if (permissions != null) attributeView.setPermissions(permissions);
            } catch (IOException e) {
                throw new RuntimeException("could not write file attributes for " + file.getPath(), e);
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
                throw new RuntimeException("could not set file times for " + file.getPath(), t);
            }

            PreserveFileAttributesFilter.removeMetadata(objectContext.getObject());
        }

        // TODO: if verification ever includes ACLs, need to display a warning that this filter cannot be reversed
        @Override
        public SyncObject reverseFilter(ObjectContext objectContext) {
            return getNext().reverseFilter(objectContext);
        }
    }
}

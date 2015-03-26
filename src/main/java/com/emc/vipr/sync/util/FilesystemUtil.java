package com.emc.vipr.sync.util;

import com.emc.vipr.sync.model.SyncMetadata;
import org.apache.log4j.Logger;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.util.Date;

public final class FilesystemUtil {
    private static final Logger l4j = Logger.getLogger(FilesystemUtil.class);

    public static final String OTHER_GROUP = "other";

    public static final String READ = "READ";
    public static final String WRITE = "WRITE";
    public static final String EXECUTE = "EXECUTE";

    public static SyncMetadata createFilesystemMetadata(File file, MimetypesFileTypeMap mimeMap, boolean includeAcl) {
        SyncMetadata metadata = new SyncMetadata();
        metadata.setContentType(mimeMap.getContentType(file));
        metadata.setSize(file.length());
        metadata.setModificationTime(new Date(file.lastModified()));

        if (includeAcl) {
/* -- requires Java 7
            SyncAcl acl = new SyncAcl();

            // build POSIX ACL
            PosixFileAttributes attributes;
            try {
                attributes = Files.readAttributes(file.toPath(), PosixFileAttributes.class);
            } catch (IOException e) {
                throw new RuntimeException("could not read file attributes for " + file.getPath(), e);
            }

            if (attributes.owner() != null) acl.setOwner(attributes.owner().getName());

            String group = null;
            if (attributes.group() != null) group = attributes.group().getName();

            MultiValueMap<String, String> userGrants = new MultiValueMap<String, String>(), groupGrants = new MultiValueMap<String, String>();
            for (PosixFilePermission permission : attributes.permissions()) {

                switch (permission) {
                    case OWNER_READ:
                    case OWNER_WRITE:
                    case OWNER_EXECUTE:
                        if (acl.getOwner() != null) userGrants.add(acl.getOwner(), fromPosixPermission(permission));
                        break;
                    case GROUP_READ:
                    case GROUP_WRITE:
                    case GROUP_EXECUTE:
                        if (group != null) groupGrants.add(group, fromPosixPermission(permission));
                        break;
                    case OTHERS_READ:
                    case OTHERS_WRITE:
                    case OTHERS_EXECUTE:
                        groupGrants.add(OTHER_GROUP, fromPosixPermission(permission));
                        break;
                }
            }
            acl.setUserGrants(userGrants);
            acl.setGroupGrants(groupGrants);

            metadata.setAcl(acl);
*/
        } // includeAcl

        return metadata;
    }

    public static void applyFilesystemMetadata(File file, SyncMetadata metadata, boolean includeAcl) {
        if (includeAcl && metadata.getAcl() != null) {
/* -- requires Java 7
            SyncAcl acl = metadata.getAcl();
            Set<PosixFilePermission> permissions = new HashSet<PosixFilePermission>();

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
*/
        }

        // set mtime (for directories, note this may be overwritten if any children are modified)
        if (metadata.getModificationTime() != null)
            file.setLastModified(metadata.getModificationTime().getTime());
    }

/* -- requires Java 7
    private static Set<PosixFilePermission> getPosixPermissions(List<String> permissions, PosixType type) {
        Set<PosixFilePermission> posixPermissions = new HashSet<PosixFilePermission>();
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
                LogMF.warn(l4j, "{0} does not map to a POSIX permission (you should use the ACL mapper)", permission);
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
*/

    private enum PosixType {
        OWNER, GROUP, OTHER
    }

    private FilesystemUtil() {
    }
}

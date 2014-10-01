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
package com.emc.vipr.sync.model;

import com.emc.atmos.api.Acl;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.api.bean.ObjectMetadata;
import com.emc.atmos.api.bean.Permission;
import com.emc.vipr.sync.util.Iso8601Util;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Similar to the Atmos API's ObjectMetadata, but it splits the system
 * metadata out into a separate collection and supports serializing to and
 * from a standard JSON format.
 *
 * @author cwikj
 */
public class AtmosMetadata extends SyncMetadata {
    private static final Logger l4j = Logger.getLogger(AtmosMetadata.class);

    private static final String TYPE_PROP = "type";
    private static final String MTIME_PROP = "mtime";
    private static final String SIZE_PROP = "size";

    private Map<String, Metadata> systemMetadata = new TreeMap<>();
    private boolean retentionEnabled;
    private Date retentionEndDate;

    private static final String[] SYSTEM_METADATA_TAGS = new String[]{
            "atime",
            "ctime",
            "gid",
            "itime",
            "mtime",
            "nlink",
            "objectid",
            "objname",
            "policyname",
            "size",
            TYPE_PROP,
            "uid",
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

    /**
     * Creates an instance of AtmosMetadata based on an ObjectMetadata
     * retrieved through the Atmos API.  This separates the system metadata
     * from the user metadata.
     *
     * @param om the Object Metadata
     * @return an AtmosMetadata
     */
    public static AtmosMetadata fromObjectMetadata(ObjectMetadata om) {
        AtmosMetadata meta = new AtmosMetadata();

        Map<String, Object> umeta = new HashMap<>();
        Map<String, Metadata> smeta = new HashMap<>();
        for (Metadata m : om.getMetadata().values()) {
            if (BAD_TAGS.contains(m.getName())) {
                // no-op
            } else if (SYSTEM_TAGS.contains(m.getName())) {
                smeta.put(m.getName(), m);
            } else {
                umeta.put(m.getName(), m);
            }
        }

        Metadata mtime = smeta.get(MTIME_PROP);
        Metadata size = smeta.get(SIZE_PROP);

        meta.setAcl(syncAclFromAtmosAcl(om.getAcl()));
        meta.setChecksum(new Checksum(om.getWsChecksum().getAlgorithm().toString(), om.getWsChecksum().getValue()));
        meta.setContentType(om.getContentType());
        if (mtime != null) meta.setModificationTime(Iso8601Util.parse(mtime.getValue()));
        if (size != null) meta.setSize(Long.parseLong(size.getValue()));
        meta.setSystemMetadata(smeta);
        meta.setUserMetadata(umeta);

        return meta;
    }

    public Map<String, Metadata> getSystemMetadata() {
        return systemMetadata;
    }

    public void setSystemMetadata(Map<String, Metadata> systemMetadata) {
        this.systemMetadata = systemMetadata;
    }

    public boolean isRetentionEnabled() {
        return retentionEnabled;
    }

    public void setRetentionEnabled(boolean retentionEnabled) {
        this.retentionEnabled = retentionEnabled;
    }

    public Date getRetentionEndDate() {
        return retentionEndDate;
    }

    public void setRetentionEndDate(Date retentionEndDate) {
        this.retentionEndDate = retentionEndDate;
    }

    public static SyncAcl syncAclFromAtmosAcl(Acl acl) {
        SyncAcl syncAcl = new SyncAcl();
        for (String user : acl.getUserAcl().keySet()) {
            syncAcl.getUserGrants().add(user, acl.getUserAcl().get(user).toString());
        }
        for (String group : acl.getGroupAcl().keySet()) {
            syncAcl.getGroupGrants().add(group, acl.getGroupAcl().get(group).toString());
        }
        return syncAcl;
    }

    public static Acl atmosAclfromSyncAcl(SyncAcl syncAcl, boolean ignoreInvalidPermissions) {
        Acl acl = new Acl();
        for (String user : syncAcl.getUserGrants().keySet()) {
            for (String permission : syncAcl.getUserGrants().get(user))
                acl.addUserGrant(user, getAtmosPermission(permission, ignoreInvalidPermissions));
        }
        for (String group : syncAcl.getGroupGrants().keySet()) {
            for (String permission : syncAcl.getGroupGrants().get(group))
                acl.addGroupGrant(group, getAtmosPermission(permission, ignoreInvalidPermissions));
        }

        return acl;
    }

    private static Permission getAtmosPermission(String permission, boolean ignoreInvalidPermissions) {
        try {
            return Permission.valueOf(permission);
        } catch (IllegalArgumentException e) {
            if (!ignoreInvalidPermissions) throw e;
            else
                LogMF.warn(l4j, "{0} does not map to an Atmos ACL permission (you should use the ACL mapper)", permission);
        }
        return null;
    }
}

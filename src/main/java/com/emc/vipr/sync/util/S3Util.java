/**
 *
 */
package com.emc.vipr.sync.util;

import com.amazonaws.services.s3.model.*;
import com.emc.vipr.sync.model.SyncAcl;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class S3Util {
    private static final Logger l4j = Logger.getLogger(S3Util.class);

    /**
     * This pattern is used to activate the S3 plugins
     */
    public static final String URI_PREFIX = "s3:";
    public static final String URI_PATTERN = "^" + URI_PREFIX + "(?:(http|https)://)?([^:]+):([a-zA-Z0-9\\+/=]+)@?(?:([^/]*?)(:[0-9]+)?)?(/.*)?$";
    public static final String PATTERN_DESC = URI_PREFIX + "[http[s]://]access_key:secret_key@[host[:port]][/root-prefix]";

    public static final String ACL_GROUP_TYPE = "Group";
    public static final String ACL_CANONICAL_USER_TYPE = "Canonical User";

    public static S3Uri parseUri(String uri) {
        Pattern p = Pattern.compile(URI_PATTERN);
        Matcher m = p.matcher(uri);
        if (!m.matches()) {
            throw new ConfigurationException(String.format("URI does not match %s pattern (%s)", URI_PREFIX, PATTERN_DESC));
        }

        S3Uri s3Uri = new S3Uri();

        s3Uri.protocol = m.group(1);
        String host = m.group(4);
        int port = -1;
        if (m.group(5) != null) {
            port = Integer.parseInt(m.group(5).substring(1));
        }

        if (host != null && !host.isEmpty()) {
            try {
                s3Uri.endpoint = new URI(s3Uri.protocol, null, host, port, null, null, null).toString();
            } catch (URISyntaxException e) {
                throw new ConfigurationException("invalid endpoint URI", e);
            }
        }

        s3Uri.accessKey = m.group(2);
        s3Uri.secretKey = m.group(3);

        if (m.group(6) != null)
            s3Uri.rootKey = m.group(6);

        return s3Uri;
    }

    public static class S3Uri {
        public String protocol;
        public String endpoint;
        public String accessKey;
        public String secretKey;
        public String rootKey;
    }

    public static AccessControlList s3AclFromSyncAcl(SyncAcl syncAcl, boolean ignoreInvalid) {
        AccessControlList s3Acl = new AccessControlList();

        s3Acl.setOwner(new Owner(syncAcl.getOwner(), syncAcl.getOwner()));

        for (String user : syncAcl.getUserGrants().keySet()) {
            Grantee grantee = new CanonicalGrantee(user);
            for (String permission : syncAcl.getUserGrants().get(user)) {
                s3Acl.grantPermission(grantee, getS3Permission(permission, ignoreInvalid));
            }
        }

        for (String group : syncAcl.getGroupGrants().keySet()) {
            Grantee grantee = GroupGrantee.parseGroupGrantee(group);
            if (grantee == null) {
                if (ignoreInvalid)
                    LogMF.warn(l4j, "{0} is not a valid S3 group", group);
                else
                    throw new RuntimeException(group + " is not a valid S3 group");
            }
            for (String permission : syncAcl.getGroupGrants().get(group)) {
                s3Acl.grantPermission(grantee, getS3Permission(permission, ignoreInvalid));
            }
        }

        return s3Acl;
    }

    public static SyncAcl syncAclFromS3Acl(AccessControlList s3Acl) {
        SyncAcl syncAcl = new SyncAcl();
        syncAcl.setOwner(s3Acl.getOwner().getId());
        for (Grant grant : s3Acl.getGrants()) {
            Grantee grantee = grant.getGrantee();
            if (grantee.getTypeIdentifier().equals(S3Util.ACL_GROUP_TYPE))
                syncAcl.getGroupGrants().add(grantee.getIdentifier(), grant.getPermission().toString());
            else if (grantee.getTypeIdentifier().equals(S3Util.ACL_CANONICAL_USER_TYPE))
                syncAcl.getUserGrants().add(grantee.getIdentifier(), grant.getPermission().toString());
        }
        return syncAcl;
    }

    private static Permission getS3Permission(String permission, boolean ignoreInvalid) {
        Permission s3Perm = Permission.parsePermission(permission);
        if (s3Perm == null) {
            if (ignoreInvalid)
                LogMF.warn(l4j, "{0} is not a valid S3 permission", permission);
            else
                throw new RuntimeException(permission + " is not a valid S3 permission");
        }
        return s3Perm;
    }

    private S3Util() {
    }
}

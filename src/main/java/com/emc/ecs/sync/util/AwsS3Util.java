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

/**
 *
 */
package com.emc.ecs.sync.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.model.SyncAcl;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.S3ObjectVersion;
import com.emc.ecs.sync.model.object.SyncObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AwsS3Util {
    private static final Logger log = LoggerFactory.getLogger(AwsS3Util.class);

    // Invalid for metadata names
    private static final char[] HTTP_SEPARATOR_CHARS = new char[]{
            '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', ' ', '\t'};

    /**
     * This pattern is used to activate the S3 plugins
     */
    public static final String URI_PREFIX = "s3:";
    public static final String URI_PATTERN = "^" + URI_PREFIX + "(?:(http|https)://)?([^:]+):([a-zA-Z0-9\\+/=]+)@?(?:([^/]*?)(:[0-9]+)?)?(?:/(.*))?$";
    public static final String PATTERN_DESC = URI_PREFIX + "[http[s]://]access_key:secret_key@[host[:port]][/root-prefix]";

    public static final String ACL_GROUP_TYPE = "Group";
    public static final String ACL_CANONICAL_USER_TYPE = "Canonical User";

    public static final String TYPE_DIRECTORY = "application/x-directory";

    public static final int MAX_PUT_SIZE_MB = 5 * 1024; // 5GB
    public static final int MIN_PART_SIZE_MB = 5;

    public static String fullPath(String bucketName, String key) {
        return bucketName + "/" + key;
    }

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
            s3Uri.endpoint = host + (port > 0 ? ":" + port : "");
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

        public String toUri() {
            String uri = URI_PREFIX;
            if (protocol != null) uri += protocol + "://";
            uri += accessKey + ":" + secretKey;
            if (endpoint != null || rootKey != null) uri += "@";
            if (endpoint != null) uri += endpoint;
            if (rootKey != null) uri += "/" + rootKey;
            return uri;
        }
    }

    public static boolean isDirectoryPlaceholder(SyncObject object) {
        return TYPE_DIRECTORY.equals(object.getMetadata().getContentType())
                && object.getMetadata().getContentLength() == 0;
    }

    public static ListIterator<S3ObjectVersion> listVersions(
            SyncPlugin parentPlugin, AmazonS3 s3, String bucket, String key, String relativePath) {
        List<S3ObjectVersion> versions = new ArrayList<>();

        VersionListing listing = null;
        do {
            if (listing == null) listing = s3.listVersions(bucket, key, null, null, "/", null);
            else listing = s3.listNextBatchOfVersions(listing);

            for (S3VersionSummary summary : listing.getVersionSummaries()) {

                if (summary.getKey().equals(key)) {
                    versions.add(new S3ObjectVersion(parentPlugin, s3, bucket, key, summary.getVersionId(),
                            summary.isLatest(), summary.isDeleteMarker(), summary.getLastModified(),
                            summary.getETag(), relativePath, summary.getSize()));
                }
            }
        } while (listing.isTruncated());

        // sort chronologically
        Collections.sort(versions, new VersionComparator());
        return versions.listIterator();
    }

    public static AccessControlList s3AclFromSyncAcl(SyncAcl syncAcl, boolean ignoreInvalid) {
        AccessControlList s3Acl = new AccessControlList();

        s3Acl.setOwner(new Owner(syncAcl.getOwner(), syncAcl.getOwner()));

        for (String user : syncAcl.getUserGrants().keySet()) {
            Grantee grantee = new CanonicalGrantee(user);
            for (String permission : syncAcl.getUserGrants().get(user)) {
                Permission perm = getS3Permission(permission, ignoreInvalid);
                if (perm != null) s3Acl.grantPermission(grantee, perm);
            }
        }

        for (String group : syncAcl.getGroupGrants().keySet()) {
            Grantee grantee = GroupGrantee.parseGroupGrantee(group);
            if (grantee == null) {
                if (ignoreInvalid)
                    log.warn("{} is not a valid S3 group", group);
                else
                    throw new RuntimeException(group + " is not a valid S3 group");
            }
            for (String permission : syncAcl.getGroupGrants().get(group)) {
                Permission perm = getS3Permission(permission, ignoreInvalid);
                if (perm != null) s3Acl.grantPermission(grantee, perm);
            }
        }

        return s3Acl;
    }

    public static SyncAcl syncAclFromS3Acl(AccessControlList s3Acl) {
        SyncAcl syncAcl = new SyncAcl();
        syncAcl.setOwner(s3Acl.getOwner().getId());
        for (Grant grant : s3Acl.getGrantsAsList()) {
            Grantee grantee = grant.getGrantee();
            if (grantee instanceof GroupGrantee || grantee.getTypeIdentifier().equals(AwsS3Util.ACL_GROUP_TYPE))
                syncAcl.addGroupGrant(grantee.getIdentifier(), grant.getPermission().toString());
            else if (grantee instanceof CanonicalGrantee || grantee.getTypeIdentifier().equals(AwsS3Util.ACL_CANONICAL_USER_TYPE))
                syncAcl.addUserGrant(grantee.getIdentifier(), grant.getPermission().toString());
        }
        return syncAcl;
    }

    public static ObjectMetadata s3MetaFromSyncMeta(SyncMetadata syncMeta) {
        ObjectMetadata om = new ObjectMetadata();
        if (syncMeta.getCacheControl() != null) om.setCacheControl(syncMeta.getCacheControl());
        if (syncMeta.getContentDisposition() != null) om.setContentDisposition(syncMeta.getContentDisposition());
        if (syncMeta.getContentEncoding() != null) om.setContentEncoding(syncMeta.getContentEncoding());
        om.setContentLength(syncMeta.getContentLength());
        if (syncMeta.getChecksum() != null && syncMeta.getChecksum().getAlgorithm().equals("MD5"))
            om.setContentMD5(syncMeta.getChecksum().getValue());
        if (syncMeta.getContentType() != null) om.setContentType(syncMeta.getContentType());
        if (syncMeta.getHttpExpires() != null) om.setHttpExpiresDate(syncMeta.getHttpExpires());
        om.setUserMetadata(formatUserMetadata(syncMeta));
        if (syncMeta.getModificationTime() != null) om.setLastModified(syncMeta.getModificationTime());
        return om;
    }

    private static Permission getS3Permission(String permission, boolean ignoreInvalid) {
        Permission s3Perm = Permission.parsePermission(permission);
        if (s3Perm == null) {
            if (ignoreInvalid)
                log.warn("{} is not a valid S3 permission", permission);
            else
                throw new RuntimeException(permission + " is not a valid S3 permission");
        }
        return s3Perm;
    }

    protected static Map<String, String> formatUserMetadata(SyncMetadata metadata) {
        Map<String, String> s3meta = new HashMap<>();

        for (String key : metadata.getUserMetadata().keySet()) {
            s3meta.put(filterName(key), filterValue(metadata.getUserMetadataValue(key)));
        }

        return s3meta;
    }

    /**
     * S3 metadata names must be compatible with header naming.  Filter the names so
     * they're acceptable.
     * Per HTTP RFC:<br>
     * <pre>
     * token          = 1*<any CHAR except CTLs or separators>
     * separators     = "(" | ")" | "<" | ">" | "@"
     *                 | "," | ";" | ":" | "\" | <">
     *                 | "/" | "[" | "]" | "?" | "="
     *                 | "{" | "}" | SP | HT
     * <pre>
     *
     * @param name the header name to filter.
     * @return the metadata name filtered to be compatible with HTTP headers.
     */
    private static String filterName(String name) {
        try {
            // First, filter out any non-ASCII characters.
            byte[] raw = name.getBytes("US-ASCII");
            String ascii = new String(raw, "US-ASCII");

            // Strip separator chars
            for (char sep : HTTP_SEPARATOR_CHARS) {
                ascii = ascii.replace(sep, '-');
            }

            return ascii;
        } catch (UnsupportedEncodingException e) {
            // should never happen
            throw new RuntimeException("Missing ASCII encoding", e);
        }
    }

    /**
     * S3 sends metadata as HTTP headers, unencoded.  Filter values to be compatible
     * with headers.
     */
    private static String filterValue(String value) {
        try {
            // First, filter out any non-ASCII characters.
            byte[] raw = value.getBytes("US-ASCII");
            String ascii = new String(raw, "US-ASCII");

            // Make sure there's no newlines
            ascii = ascii.replace('\n', ' ');

            return ascii;
        } catch (UnsupportedEncodingException e) {
            // should never happen
            throw new RuntimeException("Missing ASCII encoding", e);
        }
    }

    private AwsS3Util() {
    }

    private static class VersionComparator implements Comparator<S3ObjectVersion> {
        @Override
        public int compare(S3ObjectVersion o1, S3ObjectVersion o2) {
            int result = o1.getLastModified().compareTo(o2.getLastModified());
            if (result == 0) result = o1.getVersionId().compareTo(o2.getVersionId());
            return result;
        }
    }
}

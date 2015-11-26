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

import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.model.SyncAcl;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.EcsS3ObjectVersion;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.*;
import com.emc.object.s3.request.ListVersionsRequest;
import com.emc.rest.smart.Host;
import com.emc.rest.smart.ecs.Vdc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class EcsS3Util {
    private static final Logger log = LoggerFactory.getLogger(EcsS3Util.class);

    /**
     * This pattern is used to activate the S3 plugins
     */
    public static final String URI_PREFIX = "ecs-s3:";
    public static final String URI_PATTERN = "^" + URI_PREFIX + "(?:(http|https)://)?([^:]+):([a-zA-Z0-9\\+/=]+)@?(?:([^/]*?)(:[0-9]+)?)?(?:/(.*))?$";
    public static final String PATTERN_DESC = URI_PREFIX + "[http[s]://]access_key:secret_key@host-spec[/root-prefix] where host-spec = vdc-name(host,..),.. or load-balancer[:port]";
    public static final String VDC_PATTERN = ",?([^(]*)[(]([^)]*)[)]";

    public static final int MIN_PART_SIZE_MB = 4;

    public static final String OPERATION_LIST_VERSIONS = "EcsS3ListVersions";

    public static S3Uri parseUri(String uri) {
        Pattern p = Pattern.compile(URI_PATTERN);
        Matcher m = p.matcher(uri);
        if (!m.matches()) {
            throw new ConfigurationException(String.format("URI does not match %s pattern (%s)", URI_PREFIX, PATTERN_DESC));
        }

        S3Uri s3Uri = new S3Uri();

        s3Uri.protocol = m.group(1);
        String hostString = m.group(4);
        s3Uri.port = -1;
        if (m.group(5) != null) s3Uri.port = Integer.parseInt(m.group(5).substring(1));

        if (hostString != null && !hostString.isEmpty()) {
            Pattern vp = Pattern.compile(VDC_PATTERN);
            Matcher vm = vp.matcher(hostString);
            s3Uri.vdcs = new ArrayList<>();
            if (vm.find()) {
                do {
                    s3Uri.vdcs.add(new Vdc(vm.group(2).split(",")).withName(vm.group(1)));
                } while (vm.find());
            } else {
                s3Uri.vdcs.add(new Vdc(hostString.split(",")));
            }
        }

        s3Uri.accessKey = m.group(2);
        s3Uri.secretKey = m.group(3);

        if (m.group(6) != null) s3Uri.rootKey = m.group(6);

        return s3Uri;
    }

    public static class S3Uri {
        public String protocol;
        public List<Vdc> vdcs;
        public int port;
        public String accessKey;
        public String secretKey;
        public String rootKey;

        public String toUri() {
            String uri = URI_PREFIX;
            if (protocol != null) uri += protocol + "://";
            uri += accessKey + ":" + secretKey;
            if (vdcs != null || rootKey != null) uri += "@";
            String portStr = "";
            if (port > 0) portStr = ":" + port;
            if (vdcs != null) {
                String vdcString = "";
                for (Vdc vdc : vdcs) {
                    if (vdcString.length() > 0) vdcString += ",";
                    String hostString = "";
                    for (Host host : vdc.getHosts()) {
                        if (hostString.length() > 0) hostString += ",";
                        hostString += host.getName();
                    }
                    vdcString += vdc.getName();
                    if (!vdc.getName().equals(hostString)) vdcString += '(' + hostString + ")";
                }
                uri += vdcString + portStr + "/";
            }
            if (rootKey != null) uri += rootKey;
            return uri;
        }

        public URI getEndpointUri() {
            String uri = "";
            if (protocol != null) uri += protocol + "://";
            String portStr = "";
            if (port > 0) portStr = ":" + port;
            if (vdcs != null && !vdcs.isEmpty()) uri += vdcs.get(0).getHosts().get(0).getName() + portStr;
            try {
                return new URI(uri);
            } catch (URISyntaxException e) {
                throw new ConfigurationException("invalid endpoint URI", e);
            }
        }
    }

    public static ListIterator<EcsS3ObjectVersion> listVersions(
            SyncPlugin parentPlugin, final S3Client s3, final String bucket, final String key, String relativePath) {
        List<EcsS3ObjectVersion> versions = new ArrayList<>();

        ListVersionsResult result = null;
        do {
            if (result == null) {
                result = TimingUtil.time(parentPlugin, OPERATION_LIST_VERSIONS, new Function<ListVersionsResult>() {
                    @Override
                    public ListVersionsResult call() {
                        return s3.listVersions(new ListVersionsRequest(bucket).withPrefix(key).withDelimiter("/"));
                    }
                });
            } else {
                final ListVersionsResult fResult = result;
                result = TimingUtil.time(parentPlugin, OPERATION_LIST_VERSIONS, new Function<ListVersionsResult>() {
                    @Override
                    public ListVersionsResult call() {
                        return s3.listMoreVersions(fResult);
                    }
                });
            }

            for (AbstractVersion version : result.getVersions()) {
                if (version.getKey().equals(key)) {
                    versions.add(new EcsS3ObjectVersion(parentPlugin, s3, bucket, key, version.getVersionId(),
                            version.isLatest(), version instanceof DeleteMarker, version.getLastModified(),
                            version instanceof Version ? ((Version) version).getETag() : null, relativePath,
                            version instanceof Version ? ((Version) version).getSize() : null));
                }
            }
        } while (result.isTruncated());

        // sort chronologically
        Collections.sort(versions, new VersionComparator());
        return versions.listIterator();
    }

    public static AccessControlList s3AclFromSyncAcl(SyncAcl syncAcl, boolean ignoreInvalid) {
        AccessControlList s3Acl = new AccessControlList();

        s3Acl.setOwner(new CanonicalUser(syncAcl.getOwner(), syncAcl.getOwner()));

        for (String user : syncAcl.getUserGrants().keySet()) {
            AbstractGrantee grantee = new CanonicalUser(user, user);
            for (String permission : syncAcl.getUserGrants().get(user)) {
                Permission perm = getS3Permission(permission, ignoreInvalid);
                if (perm != null) s3Acl.addGrants(new Grant(grantee, perm));
            }
        }

        for (String group : syncAcl.getGroupGrants().keySet()) {
            AbstractGrantee grantee = new Group(group);
            for (String permission : syncAcl.getGroupGrants().get(group)) {
                Permission perm = getS3Permission(permission, ignoreInvalid);
                if (perm != null) s3Acl.addGrants(new Grant(grantee, perm));
            }
        }

        return s3Acl;
    }

    private static Permission getS3Permission(String permission, boolean ignoreInvalid) {
        Permission s3Perm = null;
        try {
            s3Perm = Permission.valueOf(permission);
        } catch (IllegalArgumentException e) {
            if (ignoreInvalid)
                log.warn("{} is not a valid S3 permission", permission);
            else
                throw new RuntimeException(permission + " is not a valid S3 permission");
        }
        return s3Perm;
    }

    public static SyncAcl syncAclFromS3Acl(AccessControlList s3Acl) {
        SyncAcl syncAcl = new SyncAcl();
        syncAcl.setOwner(s3Acl.getOwner().getId());
        for (Grant grant : s3Acl.getGrants()) {
            AbstractGrantee grantee = grant.getGrantee();
            if (grantee instanceof Group)
                syncAcl.addGroupGrant(((Group) grantee).getUri(), grant.getPermission().toString());
            else if (grantee instanceof CanonicalUser)
                syncAcl.addUserGrant(((CanonicalUser) grantee).getId(), grant.getPermission().toString());
        }
        return syncAcl;
    }

    public static S3ObjectMetadata s3MetaFromSyncMeta(SyncMetadata syncMeta) {
        S3ObjectMetadata om = new S3ObjectMetadata();
        if (syncMeta.getCacheControl() != null) om.setCacheControl(syncMeta.getCacheControl());
        if (syncMeta.getContentDisposition() != null) om.setContentDisposition(syncMeta.getContentDisposition());
        if (syncMeta.getContentEncoding() != null) om.setContentEncoding(syncMeta.getContentEncoding());
        om.setContentLength(syncMeta.getContentLength());
        if (syncMeta.getChecksum() != null && syncMeta.getChecksum().getAlgorithm().equals("MD5"))
            om.setContentMd5(syncMeta.getChecksum().getValue());
        if (syncMeta.getContentType() != null) om.setContentType(syncMeta.getContentType());
        if (syncMeta.getHttpExpires() != null) om.setHttpExpires(syncMeta.getHttpExpires());
        om.setUserMetadata(AwsS3Util.formatUserMetadata(syncMeta));
        if (syncMeta.getModificationTime() != null) om.setLastModified(syncMeta.getModificationTime());
        return om;
    }

    private EcsS3Util() {
    }

    private static class VersionComparator implements Comparator<EcsS3ObjectVersion> {
        @Override
        public int compare(EcsS3ObjectVersion o1, EcsS3ObjectVersion o2) {
            int result = o1.getLastModified().compareTo(o2.getLastModified());
            if (result == 0) result = o1.getVersionId().compareTo(o2.getVersionId());
            return result;
        }
    }
}

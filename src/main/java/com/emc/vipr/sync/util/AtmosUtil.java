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
package com.emc.vipr.sync.util;

import com.emc.atmos.api.bean.Metadata;
import com.emc.vipr.sync.model.AtmosMetadata;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AtmosUtil {
    private static final Logger l4j = Logger.getLogger(AtmosUtil.class);

    /**
     * This pattern is used to activate the Atmos plugins.
     */
    public static final String URI_PREFIX = "atmos:";
    public static final String URI_PATTERN = "^" + URI_PREFIX + "(https?)://([^:]+):([a-zA-Z0-9\\+/=]+)@([^/]*?)(:[0-9]+)?(/.*)?$";
    public static final String PATTERN_DESC = URI_PREFIX + "http[s]://uid:secret@host[,host..][:port][/namespace-path]";

    public static final String DIRECTORY_TYPE = "directory";
    public static final String TYPE_KEY = "type";

    public static List<Metadata> getRetentionMetadataForUpdate(SyncObject<?> object) {
        List<Metadata> list = new ArrayList<>();

        Date retentionEnd = getRetentionEndDate(object.getMetadata());

        if (retentionEnd != null) {
            LogMF.debug(l4j, "Retention {0} (OID: {1}, end-date: {2})", "enabled",
                    object.getSourceIdentifier(), Iso8601Util.format(retentionEnd));
            list.add(new Metadata("user.maui.retentionEnable", "true", false));
            list.add(new Metadata("user.maui.retentionEnd", Iso8601Util.format(retentionEnd), false));
        }

        return list;
    }

    public static List<Metadata> getExpirationMetadataForUpdate(SyncObject<?> object) {
        List<Metadata> list = new ArrayList<>();

        Date expiration = object.getMetadata().getExpirationDate();

        if (expiration != null) {
            LogMF.debug(l4j, "Expiration {0} (OID: {1}, end-date: {2})", "enabled",
                    object.getSourceIdentifier(), Iso8601Util.format(expiration));
            list.add(new Metadata("user.maui.expirationEnable", "true", false));
            list.add(new Metadata("user.maui.expirationEnd", Iso8601Util.format(expiration), false));
        }

        return list;
    }

    public static Date getRetentionEndDate(SyncMetadata metadata) {
        if (metadata instanceof AtmosMetadata) return ((AtmosMetadata) metadata).getRetentionEndDate();
        return null;
    }

    public static Map<String, Metadata> getAtmosUserMetadata(SyncMetadata metadata) {
        Map<String, Metadata> userMetadata = new HashMap<>();
        for (SyncMetadata.UserMetadata uMeta : metadata.getUserMetadata().values()) {
            userMetadata.put(uMeta.getKey(), new Metadata(uMeta.getKey(), uMeta.getValue(), uMeta.isIndexed()));
        }
        return userMetadata;
    }

    public static AtmosUri parseUri(String uri) {
        Pattern p = Pattern.compile(URI_PATTERN);
        Matcher m = p.matcher(uri);
        if (!m.matches()) {
            throw new ConfigurationException(String.format("URI does not match %s pattern (%s)", URI_PREFIX, PATTERN_DESC));
        }

        AtmosUri atmosUri = new AtmosUri();

        String protocol = m.group(1);
        String[] hosts = m.group(4).split(",");
        int port = -1;
        if (m.group(5) != null) {
            port = Integer.parseInt(m.group(5).substring(1));
        }

        try {
            atmosUri.endpoints = new ArrayList<>();
            for (String host : hosts) {
                atmosUri.endpoints.add(new URI(protocol, null, host, port, null, null, null));
            }
        } catch (URISyntaxException e) {
            throw new ConfigurationException("invalid endpoint URI", e);
        }

        atmosUri.uid = m.group(2);
        atmosUri.secret = m.group(3);

        if (m.group(6) != null)
            atmosUri.rootPath = m.group(6);

        return atmosUri;
    }

    private AtmosUtil() {
    }

    public static class AtmosUri {
        public List<URI> endpoints;
        public String uid;
        public String secret;
        public String rootPath;
    }
}

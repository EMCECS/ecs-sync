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
package com.emc.ecs.sync.config.storage;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.annotation.*;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.emc.ecs.sync.config.storage.AtmosConfig.PATTERN_DESC;
import static com.emc.ecs.sync.config.storage.AtmosConfig.URI_PREFIX;

@XmlRootElement
@StorageConfig(uriPrefix = URI_PREFIX)
@Label("Atmos")
@Documentation("The Atmos plugin is triggered by the URI pattern:\n" +
        PATTERN_DESC + "\n" +
        "Note that the uid should be the 'full token ID' including the " +
        "subtenant ID and the uid concatenated by a slash\n" +
        "If you want to software load balance across multiple hosts, " +
        "you can provide a comma-delimited list of hostnames or IPs " +
        "in the host part of the URI.")
public class AtmosConfig extends AbstractConfig {
    public static final String URI_PREFIX = "atmos:";
    public static final Pattern URI_PATTERN = Pattern.compile("^" + URI_PREFIX + "(https?)://([^:]+):([a-zA-Z0-9\\+/=]+)@([^/]*?)(:[0-9]+)?(/.*)?$");
    public static final String PATTERN_DESC = "atmos:http[s]://uid:secret@host[,host..][:port][/namespace-path]";

    private Protocol protocol = Protocol.https;
    private String[] hosts;
    private int port = -1;
    private String uid;
    private String secret;
    private String path;
    private AccessType accessType = AccessType.namespace;
    private boolean removeTagsOnDelete;
    private Hash wsChecksumType;
    private boolean replaceMetadata;

    @UriGenerator
    public String getUri() {
        String portStr = port > 0 ? ":" + port : "";
        String pathStr = path == null ? "" : path;
        return String.format("%s%s://%s:%s@%s%s%s", URI_PREFIX, protocol, uid, secret, ConfigUtil.join(hosts), portStr, pathStr);
    }

    @UriParser
    public void setUri(String uri) {
        Matcher m = URI_PATTERN.matcher(uri);
        if (!m.matches()) {
            throw new ConfigurationException(String.format("URI does not match %s pattern (%s)", URI_PREFIX, PATTERN_DESC));
        }

        protocol = Protocol.valueOf(m.group(1).toLowerCase());
        hosts = m.group(4).split(",");
        port = -1;
        if (m.group(5) != null) port = Integer.parseInt(m.group(5).substring(1));

        uid = m.group(2);
        secret = m.group(3);

        path = m.group(6);

        if (hosts[0].length() == 0 || uid == null || secret == null)
            throw new ConfigurationException("protocol, host[s], uid and secret are required");
    }

    @Option(locations = Option.Location.Form, required = true, description = "The protocol to use when connecting to Atmos (http or https)")
    public Protocol getProtocol() {
        return protocol;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    @Option(locations = Option.Location.Form, required = true, description = "The set of hosts (or a single load-balancer) to use when connecting to Atmos")
    public String[] getHosts() {
        return hosts;
    }

    public void setHosts(String[] hosts) {
        this.hosts = hosts;
    }

    @Option(locations = Option.Location.Form, required = true, description = "The port to use when connecting to Atmos")
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Option(locations = Option.Location.Form, required = true, description = "The full uid string (<subtenant_id>/<uid>)", valueHint = "subtenant_id/uid")
    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Option(locations = Option.Location.Form, required = true, description = "The secret key for the specified uid")
    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    @Option(locations = Option.Location.Form, description = "The path within the subtenant namespace to sync (optional). If not specified, the entire namespace will be synced")
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Option(description = "The access method to locate objects (objectspace or namespace)")
    public AccessType getAccessType() {
        return accessType;
    }

    public void setAccessType(AccessType accessType) {
        this.accessType = accessType;
    }

    @Option(description = "When deleting from a source subtenant, specifies whether to delete listable-tags prior to deleting the object. This is done to reduce the tag index size and improve write performance under the same tags")
    public boolean isRemoveTagsOnDelete() {
        return removeTagsOnDelete;
    }

    public void setRemoveTagsOnDelete(boolean removeTagsOnDelete) {
        this.removeTagsOnDelete = removeTagsOnDelete;
    }

    @Option(description = "If specified, the atmos wschecksum feature will be applied to writes. Valid algorithms are sha1, or md5. Disabled by default")
    public Hash getWsChecksumType() {
        return wsChecksumType;
    }

    public void setWsChecksumType(Hash wsChecksumType) {
        this.wsChecksumType = wsChecksumType;
    }

    @Option(description = "Atmos does not have a call to replace metadata; only to set or remove it. By default, set is used, which means removed metadata will not be reflected when updating objects. Use this flag if your sync operation might remove metadata from an existing object")
    public boolean isReplaceMetadata() {
        return replaceMetadata;
    }

    public void setReplaceMetadata(boolean replaceMetadata) {
        this.replaceMetadata = replaceMetadata;
    }

    @XmlType(namespace = "http://www.emc.com/ecs/sync/model")
    public enum AccessType {
        objectspace, namespace
    }

    @XmlType(namespace = "http://www.emc.com/ecs/sync/model")
    public enum Hash {
        md5, sha1
    }
}

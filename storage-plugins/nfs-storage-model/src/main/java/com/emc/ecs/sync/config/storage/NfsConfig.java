/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.config.storage;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.RoleType;
import com.emc.ecs.sync.config.annotation.*;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.emc.ecs.sync.config.storage.NfsConfig.URI_PREFIX;

@XmlRootElement
@StorageConfig(uriPrefix = URI_PREFIX)
@Label("NFS")
@Documentation("The nfs plugin reads/writes data from/to an nfs file or directory. " +
        "It is triggered by the URI:\n" +
        "nfs://server/<mount_root_path>, e.g." +
        "nfs://myserver/home/user/myfiles.\n" +
        "If <code>subPath</code> refers to a file, only that file will be " +
        "synced. If a directory is specified, the contents of " +
        "the directory will be synced.  Unless the --non-recursive " +
        "flag is set, the subdirectories will also be recursively " +
        "synced. To preserve object metadata on the target filesystem, " +
        "or to read back preserved metadata, use --store-metadata.")
public class NfsConfig extends AbstractConfig {
    static final String URI_PREFIX = "nfs:";
    private static final Pattern URI_PATTERN = Pattern.compile("^nfs://([^/]+)(/[^?]*)$");

    protected String server;
    protected String mountPath;
    protected String subPath;
    private boolean followLinks = false;
    private boolean storeMetadata = false;
    private long deleteOlderThan = 0;
    private String modifiedSince;
    private String[] excludedPaths;

    @UriGenerator
    public String getUri(boolean scrubbed) {
        return URI_PREFIX + "//" + bin(server) + bin(mountPath);
    }

    @UriParser
    public void setUri(String uri) {
        Matcher matcher = URI_PATTERN.matcher(uri);
        if (matcher.matches()) {
            setServer(matcher.group(1));
            setMountPath(matcher.group(2));
        } else {
            throw new RuntimeException("invalid file URI");
        }
    }

    @Option(orderIndex = 10, locations = Option.Location.Form, required = true, description = "Nfs mount server.")
    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    @Option(orderIndex = 20, locations = Option.Location.Form, required = true, description = "Path to the mount root.")
    public String getMountPath() {
        return mountPath;
    }

    public void setMountPath(String path) {
        this.mountPath = path;
    }

    @Option(orderIndex = 30, description = "Path to the primary file or directory from the mount root.")
    public String getSubPath() {
        return subPath;
    }

    public void setSubPath(String path) {
        this.subPath = path;
    }

    @Role(RoleType.Source)
    @Option(orderIndex = 40, advanced = true, description = "Instead of preserving symbolic links, follow them and sync the actual files")
    public boolean isFollowLinks() {
        return followLinks;
    }

    public void setFollowLinks(boolean followLinks) {
        this.followLinks = followLinks;
    }

    @Role(RoleType.Target)
    @Option(orderIndex = 50, advanced = true, description = "When used as a target, stores source metadata in a json file, since NFS filesystems have no concept of user metadata")
    public boolean isStoreMetadata() {
        return storeMetadata;
    }

    public void setStoreMetadata(boolean storeMetadata) {
        this.storeMetadata = storeMetadata;
    }

    @Role(RoleType.Source)
    @Option(orderIndex = 60, advanced = true, valueHint = "delete-age", description = "When --delete-source is used, add this option to only delete files that have been modified more than <delete-age> milliseconds ago")
    public long getDeleteOlderThan() {
        return deleteOlderThan;
    }

    public void setDeleteOlderThan(long deleteOlderThan) {
        this.deleteOlderThan = deleteOlderThan;
    }

    @Role(RoleType.Source)
    @Option(orderIndex = 70, advanced = true, valueHint = "yyyy-MM-ddThh:mm:ssZ", description = "Only look at files that have been modified since the specific date/time.  Date/time should be provided in ISO-8601 UTC format (i.e. 2015-01-01T04:30:00Z)")
    public String getModifiedSince() {
        return modifiedSince;
    }

    /**
     * Date/time should be provided in ISO-8601 UTC format (i.e. 2015-01-01T04:30:00Z)
     */
    public void setModifiedSince(String modifiedSince) {
        this.modifiedSince = modifiedSince;
    }

    @Role(RoleType.Source)
    @Option(orderIndex = 80, valueHint = "regex-pattern", description = "A list of regular expressions to search against the full file path.  If the path matches, the file will be skipped.  Since this is a regular expression, take care to escape special characters.  For example, to exclude all .snapshot directories, the pattern would be .*/\\.snapshot. Specify multiple entries by repeating the CLI option or using multiple lines in the UI form")
    public String[] getExcludedPaths() {
        return excludedPaths;
    }

    public void setExcludedPaths(String[] excludedPaths) {
        this.excludedPaths = excludedPaths;
    }
}

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

import com.emc.ecs.sync.config.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.emc.ecs.sync.config.storage.ArchiveConfig.URI_PREFIX;

@XmlRootElement
@StorageConfig(uriPrefix = URI_PREFIX)
@Label("Archive File")
@Documentation("The archive plugin reads/writes data from/to an archive file (tar, zip, etc.) " +
        "It is triggered by an archive URL:\n" +
        "archive:[<scheme>://]<path>, e.g. archive:file:///home/user/myfiles.tar\n" +
        "or archive:http://company.com/bundles/project.tar.gz or archive:cwd_file.zip\n" +
        "The contents of the archive are the objects. " +
        "To preserve object metadata on the target filesystem, " +
        "or to read back preserved metadata, use --store-metadata.")
public class ArchiveConfig extends FilesystemConfig {
    private static final Logger log = LoggerFactory.getLogger(ArchiveConfig.class);

    static final String URI_PREFIX = "archive:";
    private static final Pattern URI_PATTERN = Pattern.compile("^archive:(?://)?(.+)$");

    @Override
    @XmlTransient
    @UriGenerator
    public String getUri() {
        return URI_PREFIX + path;
    }

    @Override
    @UriParser
    public void setUri(String uri) {
        Matcher matcher = URI_PATTERN.matcher(uri);
        if (matcher.matches()) {
            path = matcher.group(1);
        } else {
            throw new RuntimeException("invalid archive URI");
        }
    }

    @Override
    public void setUseAbsolutePath(boolean useAbsolutePath) {
        if (useAbsolutePath) log.warn("Archive sources will always use a relative path!");
    }
}

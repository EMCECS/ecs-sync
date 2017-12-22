/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.config.filter;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;
import com.emc.ecs.sync.config.annotation.Option;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@FilterConfig(cliName = "local-cache")
@Label("Local Cache")
@Documentation("Writes each object to a local cache directory before writing to the target. Useful for applying " +
        "external transformations or for transforming objects in-place (source/target are the same)\n" +
        "NOTE: this filter will remove any extended properties from storage plugins (i.e. versions, CAS tags, etc.) " +
        "Do not use this plugin if you are using those features")
public class LocalCacheConfig extends AbstractConfig {
    private String localCacheRoot;

    @Option(orderIndex = 10, required = true, valueHint = "cache-directory", description = "specifies the root directory in which to cache files")
    public String getLocalCacheRoot() {
        return localCacheRoot;
    }

    public void setLocalCacheRoot(String localCacheRoot) {
        this.localCacheRoot = localCacheRoot;
    }
}

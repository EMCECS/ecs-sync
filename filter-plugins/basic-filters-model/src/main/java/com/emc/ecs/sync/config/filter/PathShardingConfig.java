/*
 * Copyright (c) 2017-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.config.filter;

import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;
import com.emc.ecs.sync.config.annotation.Option;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@FilterConfig(cliName = "path-sharding")
@Label("Path Sharding Filter")
@Documentation("Shards the relative path of an object based on the MD5 of the existing path (i.e. \"a1/fe/my-identifier\"). " +
        "Useful when migrating a flat list of many identifiers to a filesystem to prevent overloading directories")
public class PathShardingConfig {
    private int shardSize = 2;
    private int shardCount = 2;
    private boolean upperCase;

    @Option(orderIndex = 10, description = "The number of hexadecimal characters in each shard directory (a value of 2 would mean each subdirectory name has 2 hex characters). I.e. a path \"my-identifier\" with shardCount of 2 and shardSize of 2 would change to \"28/24/my-identifier\"")
    public int getShardSize() {
        return shardSize;
    }

    public void setShardSize(int shardSize) {
        this.shardSize = shardSize;
    }

    @Option(orderIndex = 20, description = "The number of shard directories (a value of 2 means two levels of subdirectories, each containing a set of hex characters from the MD5). I.e. a path \"my-identifier\" with shardCount of 2 and shardSize of 2 would change to \"28/24/my-identifier\"")
    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    @Option(orderIndex = 30, advanced = true, description = "By default, shard directories will be in lower-case. Enable this to make them upper-case")
    public boolean isUpperCase() {
        return upperCase;
    }

    public void setUpperCase(boolean upperCase) {
        this.upperCase = upperCase;
    }
}

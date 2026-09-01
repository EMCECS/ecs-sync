/*
 * Copyright (c) 2024 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.cluster;

import com.emc.ecs.sync.config.ConfigurationException;

import java.util.List;

/**
 * Maps a source replication group to one on the target. Replication group IDs are cluster-specific URNs, so the only
 * portable correlation is the name.
 */
public class ReplicationGroupResolver {
    private final List<ReplicationGroup> targetGroups;
    private final String overrideName;

    public ReplicationGroupResolver(List<ReplicationGroup> targetGroups, String overrideName) {
        this.targetGroups = targetGroups;
        this.overrideName = overrideName;
        if (targetGroups.isEmpty())
            throw new ConfigurationException("the target cluster has no replication groups defined");
        if (overrideName != null && findByName(overrideName) == null) {
            throw new ConfigurationException("no replication group named '" + overrideName
                    + "' on the target cluster; available: " + names());
        }
    }

    /**
     * Matches on the group name, falling back to the ID. The UI selects a group from a list where the ID is the
     * stable identifier, while the CLI takes a human-readable name.
     */
    private ReplicationGroup find(String nameOrId) {
        for (ReplicationGroup group : targetGroups) {
            if (nameOrId.equals(group.getName())) return group;
        }
        for (ReplicationGroup group : targetGroups) {
            if (nameOrId.equals(group.getId())) return group;
        }
        return null;
    }

    /**
     * @param sourceGroupName the name of the source bucket's replication group (may be null)
     * @return the target replication group ID to use
     */
    public String resolve(String sourceGroupName) {
        if (overrideName != null) return findByName(overrideName).getId();
        if (sourceGroupName != null) {
            ReplicationGroup match = findByName(sourceGroupName);
            if (match != null) return match.getId();
        }
        if (targetGroups.size() == 1) return targetGroups.get(0).getId();
        throw new ConfigurationException(String.format(
                "cannot determine which target replication group to use for source group '%s'; the target has %s - "
                        + "specify one explicitly with the target-replication-group option",
                sourceGroupName, names()));
    }

    private ReplicationGroup findByName(String name) {
        return find(name);
    }

    private String names() {
        StringBuilder builder = new StringBuilder();
        for (ReplicationGroup group : targetGroups) {
            if (builder.length() > 0) builder.append(", ");
            builder.append(group.getName());
        }
        return builder.toString();
    }
}

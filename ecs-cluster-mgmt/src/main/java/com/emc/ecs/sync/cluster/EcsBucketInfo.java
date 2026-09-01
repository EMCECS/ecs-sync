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

/**
 * An ECS bucket, as returned by the management API.
 */
public class EcsBucketInfo {
    private String name;
    private String namespace;
    private String owner;
    private String replicationGroupId;
    private String replicationGroupName;
    private boolean versioningEnabled;
    private boolean fileSystemEnabled;

    public EcsBucketInfo() {
    }

    public EcsBucketInfo(String name, String namespace) {
        this.name = name;
        this.namespace = namespace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     * Replication group ID on the <em>source</em> cluster. This is not portable to another cluster; it is resolved to
     * a name and then re-resolved to the target cluster's ID.
     */
    public String getReplicationGroupId() {
        return replicationGroupId;
    }

    public void setReplicationGroupId(String replicationGroupId) {
        this.replicationGroupId = replicationGroupId;
    }

    public String getReplicationGroupName() {
        return replicationGroupName;
    }

    public void setReplicationGroupName(String replicationGroupName) {
        this.replicationGroupName = replicationGroupName;
    }

    public boolean isVersioningEnabled() {
        return versioningEnabled;
    }

    public void setVersioningEnabled(boolean versioningEnabled) {
        this.versioningEnabled = versioningEnabled;
    }

    public boolean isFileSystemEnabled() {
        return fileSystemEnabled;
    }

    public void setFileSystemEnabled(boolean fileSystemEnabled) {
        this.fileSystemEnabled = fileSystemEnabled;
    }

    @Override
    public String toString() {
        return "EcsBucketInfo{name='" + name + "', namespace='" + namespace + "', owner='" + owner + "'}";
    }
}

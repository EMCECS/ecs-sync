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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Top-level configuration for a whole-cluster replication. This is deliberately separate from
 * {@link com.emc.ecs.sync.config.SyncConfig}: a cluster replication is not a single source-to-target object stream,
 * it is an orchestration that produces many individual sync jobs (one per bucket).
 */
@XmlRootElement(name = "clusterSyncConfig")
@XmlAccessorType(XmlAccessType.PROPERTY)
public class ClusterSyncConfig {
    private ClusterEndpoint source;
    private ClusterEndpoint target;
    private ClusterSyncOptions options = new ClusterSyncOptions();

    public void validate() {
        if (source == null) throw new ConfigurationException("source cluster is required");
        if (target == null) throw new ConfigurationException("target cluster is required");
        source.validate("source");
        target.validate("target");
        if (options == null) options = new ClusterSyncOptions();
        if (options.getThreadCount() < 1) throw new ConfigurationException("threadCount must be at least 1");
        if (options.getBucketParallelism() < 1)
            throw new ConfigurationException("bucketParallelism must be at least 1");
        options.parseNamespaceMap(); // fail fast on a malformed mapping
        if (isSameCluster()) {
            // a same-cluster run is a storage pool migration: every namespace is recreated under a new name (see
            // NamespaceMapper), so it is only meaningful if the data lands in a different replication group
            if (options.getTargetReplicationGroup() == null) {
                throw new ConfigurationException("source and target are the same cluster, so a target replication "
                        + "group must be specified - otherwise the copy would land in the same storage pool");
            }
            if (options.isSkipNamespaceCreation()) {
                throw new ConfigurationException("namespace creation cannot be skipped for a same-cluster run, "
                        + "because each namespace must be recreated under a new name");
            }
        }
    }

    /**
     * True when both endpoints address the same cluster, i.e. a storage pool migration within one cluster.
     */
    public boolean isSameCluster() {
        return source.getManagementEndpoint().equals(target.getManagementEndpoint())
                && source.getManagementPort() == target.getManagementPort();
    }

    public ClusterEndpoint getSource() {
        return source;
    }

    public void setSource(ClusterEndpoint source) {
        this.source = source;
    }

    public ClusterEndpoint getTarget() {
        return target;
    }

    public void setTarget(ClusterEndpoint target) {
        this.target = target;
    }

    public ClusterSyncOptions getOptions() {
        return options;
    }

    public void setOptions(ClusterSyncOptions options) {
        this.options = options;
    }
}

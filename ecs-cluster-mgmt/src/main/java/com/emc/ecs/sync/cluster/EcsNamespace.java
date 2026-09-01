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
 * An ECS namespace, as returned by the management API.
 */
public class EcsNamespace {
    private String name;
    private String id;
    private String defaultDataServicesVpool;

    public EcsNamespace() {
    }

    public EcsNamespace(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The ID of the replication group that is the default for this namespace. Note that replication group IDs are
     * <em>not</em> portable between clusters - see {@link ReplicationGroup}.
     */
    public String getDefaultDataServicesVpool() {
        return defaultDataServicesVpool;
    }

    public void setDefaultDataServicesVpool(String defaultDataServicesVpool) {
        this.defaultDataServicesVpool = defaultDataServicesVpool;
    }

    @Override
    public String toString() {
        return "EcsNamespace{name='" + name + "', id='" + id + "'}";
    }
}

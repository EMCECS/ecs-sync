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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The tenancy structure discovered on the source cluster, in the order it must be reproduced on the target:
 * namespaces, then users, then buckets.
 */
public class ClusterReplicationPlan {
    private final List<EcsNamespace> namespaces = new ArrayList<>();
    private final Map<String, List<EcsObjectUser>> usersByNamespace = new LinkedHashMap<>();
    private final Map<String, List<EcsBucketInfo>> bucketsByNamespace = new LinkedHashMap<>();

    public void addNamespace(EcsNamespace namespace, List<EcsObjectUser> users, List<EcsBucketInfo> buckets) {
        namespaces.add(namespace);
        usersByNamespace.put(namespace.getName(), users);
        bucketsByNamespace.put(namespace.getName(), buckets);
    }

    public List<EcsNamespace> getNamespaces() {
        return namespaces;
    }

    public List<EcsObjectUser> getUsers(String namespace) {
        return usersByNamespace.getOrDefault(namespace, new ArrayList<>());
    }

    public List<EcsBucketInfo> getBuckets(String namespace) {
        return bucketsByNamespace.getOrDefault(namespace, new ArrayList<>());
    }

    public List<EcsBucketInfo> getAllBuckets() {
        List<EcsBucketInfo> all = new ArrayList<>();
        bucketsByNamespace.values().forEach(all::addAll);
        return all;
    }

    public int getUserCount() {
        return usersByNamespace.values().stream().mapToInt(List::size).sum();
    }

    public int getBucketCount() {
        return bucketsByNamespace.values().stream().mapToInt(List::size).sum();
    }

    /**
     * Chooses the identity used to read a bucket on the source and write it on the target. The bucket owner is
     * preferred so that ownership is preserved; otherwise any user in the namespace is used.
     *
     * @return the user id, or null if the namespace has no object users at all
     */
    public String resolveBucketUser(EcsBucketInfo bucket) {
        List<EcsObjectUser> users = getUsers(bucket.getNamespace());
        if (bucket.getOwner() != null) {
            for (EcsObjectUser user : users) {
                if (bucket.getOwner().equals(user.getUserId())) return user.getUserId();
            }
        }
        return users.isEmpty() ? null : users.get(0).getUserId();
    }
}

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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class ClusterReplicationPlanTest {
    private static ClusterReplicationPlan planWithUsers(EcsObjectUser... users) {
        ClusterReplicationPlan plan = new ClusterReplicationPlan();
        plan.addNamespace(new EcsNamespace("ns1"), Arrays.asList(users),
                Collections.singletonList(new EcsBucketInfo("bucket1", "ns1")));
        return plan;
    }

    @Test
    public void testBucketOwnerIsPreferred() {
        ClusterReplicationPlan plan = planWithUsers(
                new EcsObjectUser("alice", "ns1"), new EcsObjectUser("bob", "ns1"));
        EcsBucketInfo bucket = new EcsBucketInfo("bucket1", "ns1");
        bucket.setOwner("bob");

        Assertions.assertEquals("bob", plan.resolveBucketUser(bucket));
    }

    @Test
    public void testFallsBackToAnyUserWhenOwnerIsUnknown() {
        ClusterReplicationPlan plan = planWithUsers(new EcsObjectUser("alice", "ns1"));
        EcsBucketInfo bucket = new EcsBucketInfo("bucket1", "ns1");
        // owner is a management user that has no object-user record
        bucket.setOwner("root");

        Assertions.assertEquals("alice", plan.resolveBucketUser(bucket));
    }

    @Test
    public void testFallsBackToAnyUserWhenOwnerIsNull() {
        ClusterReplicationPlan plan = planWithUsers(new EcsObjectUser("alice", "ns1"));
        Assertions.assertEquals("alice", plan.resolveBucketUser(new EcsBucketInfo("bucket1", "ns1")));
    }

    @Test
    public void testReturnsNullWhenNamespaceHasNoUsers() {
        ClusterReplicationPlan plan = planWithUsers();
        Assertions.assertNull(plan.resolveBucketUser(new EcsBucketInfo("bucket1", "ns1")));
    }

    @Test
    public void testCounts() {
        ClusterReplicationPlan plan = new ClusterReplicationPlan();
        plan.addNamespace(new EcsNamespace("ns1"),
                Arrays.asList(new EcsObjectUser("a", "ns1"), new EcsObjectUser("b", "ns1")),
                Arrays.asList(new EcsBucketInfo("b1", "ns1"), new EcsBucketInfo("b2", "ns1")));
        plan.addNamespace(new EcsNamespace("ns2"),
                Collections.singletonList(new EcsObjectUser("c", "ns2")),
                Collections.singletonList(new EcsBucketInfo("b3", "ns2")));

        Assertions.assertEquals(2, plan.getNamespaces().size());
        Assertions.assertEquals(3, plan.getUserCount());
        Assertions.assertEquals(3, plan.getBucketCount());
        Assertions.assertEquals(3, plan.getAllBuckets().size());
        Assertions.assertTrue(plan.getUsers("no-such-namespace").isEmpty());
    }
}

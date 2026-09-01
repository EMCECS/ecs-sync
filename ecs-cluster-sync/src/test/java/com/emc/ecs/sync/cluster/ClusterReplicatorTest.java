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

public class ClusterReplicatorTest {
    @Test
    public void testDbTableNameIsSanitized() {
        EcsBucketInfo bucket = new EcsBucketInfo("my.bucket-name", "my-namespace");
        Assertions.assertEquals("cluster_sync_my_namespace_my_bucket_name",
                ClusterReplicator.dbTableName(bucket, "my-namespace"));
    }

    @Test
    public void testDbTableNameIsKeyedOnTargetNamespace() {
        // the same source bucket copied into two different target namespaces must not share status tables
        EcsBucketInfo bucket = new EcsBucketInfo("bucket1", "ns1");
        Assertions.assertNotEquals(
                ClusterReplicator.dbTableName(bucket, "ns1_target"),
                ClusterReplicator.dbTableName(bucket, "ns1_target2"));
    }

    @Test
    public void testDbTableNameFitsWithinIdentifierLimit() {
        String longNamespace = repeat("n", 60);
        String longBucket = repeat("b", 60);
        String table = ClusterReplicator.dbTableName(new EcsBucketInfo(longBucket, longNamespace), longNamespace);

        Assertions.assertTrue(table.length() <= 64, "table name too long: " + table.length());
        Assertions.assertTrue(table.matches("[A-Za-z0-9_]+"), "unexpected characters: " + table);
    }

    @Test
    public void testDbTableNamesStayDistinctAfterTruncation() {
        String longNamespace = repeat("n", 60);
        String tableA = ClusterReplicator.dbTableName(
                new EcsBucketInfo(repeat("a", 60), longNamespace), longNamespace);
        String tableB = ClusterReplicator.dbTableName(
                new EcsBucketInfo(repeat("b", 60), longNamespace), longNamespace);

        Assertions.assertNotEquals(tableA, tableB);
    }

    private static String repeat(String value, int count) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) builder.append(value);
        return builder.toString();
    }
}

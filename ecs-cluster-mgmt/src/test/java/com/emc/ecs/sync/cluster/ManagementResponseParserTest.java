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

import java.util.List;

/**
 * Verifies parsing of ECS management API payloads. The JSON here mirrors the documented response shapes.
 */
public class ManagementResponseParserTest {
    @Test
    public void testParseNamespaces() {
        String json = "{\"namespace\":["
                + "{\"id\":\"ns1\",\"name\":\"ns1\",\"link\":{\"rel\":\"self\"}},"
                + "{\"id\":\"ns2\",\"name\":\"ns2\"}]}";

        List<EcsNamespace> namespaces = ManagementResponseParser.parseNamespaces(
                ManagementResponseParser.toObject(json));

        Assertions.assertEquals(2, namespaces.size());
        Assertions.assertEquals("ns1", namespaces.get(0).getName());
        Assertions.assertEquals("ns2", namespaces.get(1).getName());
    }

    @Test
    public void testParseNamespacesFallsBackToIdWhenNameAbsent() {
        String json = "{\"namespace\":[{\"id\":\"ns-only-id\"}]}";

        List<EcsNamespace> namespaces = ManagementResponseParser.parseNamespaces(
                ManagementResponseParser.toObject(json));

        Assertions.assertEquals(1, namespaces.size());
        Assertions.assertEquals("ns-only-id", namespaces.get(0).getName());
    }

    @Test
    public void testParseEmptyListing() {
        Assertions.assertTrue(ManagementResponseParser.parseNamespaces(
                ManagementResponseParser.toObject("{}")).isEmpty());
        Assertions.assertTrue(ManagementResponseParser.parseObjectUsers(
                ManagementResponseParser.toObject("{}")).isEmpty());
        Assertions.assertTrue(ManagementResponseParser.parseBuckets(
                ManagementResponseParser.toObject("{}")).isEmpty());
    }

    @Test
    public void testParseNamespaceDetail() {
        String json = "{\"id\":\"ns1\",\"name\":\"ns1\","
                + "\"default_data_services_vpool\":\"urn:storageos:ReplicationGroupInfo:abc:global\"}";

        EcsNamespace namespace = ManagementResponseParser.parseNamespaceDetail(
                ManagementResponseParser.toObject(json));

        Assertions.assertEquals("ns1", namespace.getName());
        Assertions.assertEquals("urn:storageos:ReplicationGroupInfo:abc:global",
                namespace.getDefaultDataServicesVpool());
    }

    @Test
    public void testParseObjectUsers() {
        String json = "{\"blobuser\":["
                + "{\"userid\":\"alice\",\"namespace\":\"ns1\"},"
                + "{\"userid\":\"bob\",\"namespace\":\"ns1\"}]}";

        List<EcsObjectUser> users = ManagementResponseParser.parseObjectUsers(
                ManagementResponseParser.toObject(json));

        Assertions.assertEquals(2, users.size());
        Assertions.assertEquals("alice", users.get(0).getUserId());
        Assertions.assertEquals("ns1", users.get(0).getNamespace());
    }

    @Test
    public void testParseBuckets() {
        String json = "{\"object_bucket\":[{"
                + "\"name\":\"bucket1\",\"namespace\":\"ns1\",\"owner\":\"alice\","
                + "\"vpool\":\"urn:storageos:ReplicationGroupInfo:abc:global\","
                + "\"fs_access_enabled\":\"false\"}]}";

        List<EcsBucketInfo> buckets = ManagementResponseParser.parseBuckets(
                ManagementResponseParser.toObject(json));

        Assertions.assertEquals(1, buckets.size());
        EcsBucketInfo bucket = buckets.get(0);
        Assertions.assertEquals("bucket1", bucket.getName());
        Assertions.assertEquals("ns1", bucket.getNamespace());
        Assertions.assertEquals("alice", bucket.getOwner());
        Assertions.assertEquals("urn:storageos:ReplicationGroupInfo:abc:global", bucket.getReplicationGroupId());
        Assertions.assertFalse(bucket.isFileSystemEnabled());
    }

    @Test
    public void testParseBucketBooleansAsStringsAndBooleans() {
        // ECS returns booleans as JSON strings in some versions and as real booleans in others
        EcsBucketInfo asString = ManagementResponseParser.parseBucketDetail(ManagementResponseParser.toObject(
                "{\"name\":\"b\",\"fs_access_enabled\":\"true\",\"is_versioning_enabled\":\"true\"}"));
        EcsBucketInfo asBoolean = ManagementResponseParser.parseBucketDetail(ManagementResponseParser.toObject(
                "{\"name\":\"b\",\"fs_access_enabled\":true,\"is_versioning_enabled\":true}"));

        Assertions.assertTrue(asString.isFileSystemEnabled());
        Assertions.assertTrue(asString.isVersioningEnabled());
        Assertions.assertTrue(asBoolean.isFileSystemEnabled());
        Assertions.assertTrue(asBoolean.isVersioningEnabled());
    }

    @Test
    public void testParseReplicationGroups() {
        String json = "{\"data_service_vpool\":["
                + "{\"id\":\"urn:storageos:ReplicationGroupInfo:aaa:global\",\"name\":\"rg1\"},"
                + "{\"id\":\"urn:storageos:ReplicationGroupInfo:bbb:global\",\"name\":\"rg2\"}]}";

        List<ReplicationGroup> groups = ManagementResponseParser.parseReplicationGroups(
                ManagementResponseParser.toObject(json));

        Assertions.assertEquals(2, groups.size());
        Assertions.assertEquals("rg1", groups.get(0).getName());
        Assertions.assertEquals("urn:storageos:ReplicationGroupInfo:aaa:global", groups.get(0).getId());
    }

    @Test
    public void testParseExistingSecretKey() {
        Assertions.assertEquals("key-one", ManagementResponseParser.parseExistingSecretKey(
                ManagementResponseParser.toObject("{\"secret_key_1\":\"key-one\",\"secret_key_2\":\"\"}")));

        // falls back to the second slot when the first has been retired
        Assertions.assertEquals("key-two", ManagementResponseParser.parseExistingSecretKey(
                ManagementResponseParser.toObject("{\"secret_key_1\":\"\",\"secret_key_2\":\"key-two\"}")));

        // no key at all
        Assertions.assertNull(ManagementResponseParser.parseExistingSecretKey(
                ManagementResponseParser.toObject("{\"secret_key_1\":\"\",\"secret_key_2\":\"\"}")));
        Assertions.assertNull(ManagementResponseParser.parseExistingSecretKey(
                ManagementResponseParser.toObject("{}")));
    }

    @Test
    public void testParseNewSecretKey() {
        Assertions.assertEquals("brand-new", ManagementResponseParser.parseNewSecretKey(
                ManagementResponseParser.toObject("{\"secret_key\":\"brand-new\",\"key_timestamp\":\"2024-01-01\"}")));
        Assertions.assertNull(ManagementResponseParser.parseNewSecretKey(
                ManagementResponseParser.toObject("{}")));
    }

    @Test
    public void testNextMarker() {
        Assertions.assertEquals("marker-1", ManagementResponseParser.nextMarker(
                ManagementResponseParser.toObject("{\"NextMarker\":\"marker-1\"}")));
        Assertions.assertNull(ManagementResponseParser.nextMarker(
                ManagementResponseParser.toObject("{\"NextMarker\":\"\"}")));
        // ECS serializes an absent marker as the literal string "null" in some versions
        Assertions.assertNull(ManagementResponseParser.nextMarker(
                ManagementResponseParser.toObject("{\"NextMarker\":\"null\"}")));
        Assertions.assertNull(ManagementResponseParser.nextMarker(ManagementResponseParser.toObject("{}")));
    }

    @Test
    public void testNonObjectResponseIsRejected() {
        Assertions.assertThrows(EcsManagementException.class, () -> ManagementResponseParser.toObject("[]"));
    }
}

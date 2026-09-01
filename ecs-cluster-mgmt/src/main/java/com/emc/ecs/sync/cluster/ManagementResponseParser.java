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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses ECS management API JSON responses. Kept free of any transport concerns so it can be unit-tested against
 * captured payloads without a live cluster.
 */
final class ManagementResponseParser {
    private ManagementResponseParser() {
    }

    static JsonObject toObject(String json) {
        JsonElement element = JsonParser.parseString(json);
        if (!element.isJsonObject()) throw new EcsManagementException("expected a JSON object but got: " + json);
        return element.getAsJsonObject();
    }

    /**
     * ECS list endpoints page with a {@code NextMarker} property. Returns null when the listing is complete.
     */
    static String nextMarker(JsonObject root) {
        String marker = string(root, "NextMarker");
        if (marker == null) marker = string(root, "next_marker");
        if (marker == null || marker.trim().isEmpty() || "null".equals(marker)) return null;
        return marker;
    }

    static List<EcsNamespace> parseNamespaces(JsonObject root) {
        List<EcsNamespace> namespaces = new ArrayList<>();
        for (JsonElement element : array(root, "namespace")) {
            JsonObject object = element.getAsJsonObject();
            EcsNamespace namespace = new EcsNamespace();
            // ECS uses the namespace name as its id in most deployments, but fall back either way
            namespace.setName(firstNonNull(string(object, "name"), string(object, "id")));
            namespace.setId(firstNonNull(string(object, "id"), string(object, "name")));
            if (namespace.getName() != null) namespaces.add(namespace);
        }
        return namespaces;
    }

    static EcsNamespace parseNamespaceDetail(JsonObject object) {
        EcsNamespace namespace = new EcsNamespace();
        namespace.setName(firstNonNull(string(object, "name"), string(object, "id")));
        namespace.setId(firstNonNull(string(object, "id"), string(object, "name")));
        namespace.setDefaultDataServicesVpool(string(object, "default_data_services_vpool"));
        return namespace;
    }

    static List<EcsObjectUser> parseObjectUsers(JsonObject root) {
        List<EcsObjectUser> users = new ArrayList<>();
        for (JsonElement element : array(root, "blobuser")) {
            JsonObject object = element.getAsJsonObject();
            String userId = firstNonNull(string(object, "userid"), string(object, "name"));
            if (userId == null) continue;
            users.add(new EcsObjectUser(userId, string(object, "namespace")));
        }
        return users;
    }

    static List<EcsBucketInfo> parseBuckets(JsonObject root) {
        List<EcsBucketInfo> buckets = new ArrayList<>();
        for (JsonElement element : array(root, "object_bucket")) {
            buckets.add(parseBucketDetail(element.getAsJsonObject()));
        }
        return buckets;
    }

    static EcsBucketInfo parseBucketDetail(JsonObject object) {
        EcsBucketInfo bucket = new EcsBucketInfo();
        bucket.setName(firstNonNull(string(object, "name"), string(object, "id")));
        bucket.setNamespace(string(object, "namespace"));
        bucket.setOwner(string(object, "owner"));
        bucket.setReplicationGroupId(firstNonNull(string(object, "vpool"), string(object, "vpool_id")));
        // present on newer ECS versions only; the copier confirms versioning over S3 regardless
        bucket.setVersioningEnabled(bool(object, "is_versioning_enabled"));
        bucket.setFileSystemEnabled(bool(object, "fs_access_enabled"));
        return bucket;
    }

    static List<ReplicationGroup> parseReplicationGroups(JsonObject root) {
        List<ReplicationGroup> groups = new ArrayList<>();
        for (JsonElement element : array(root, "data_service_vpool")) {
            JsonObject object = element.getAsJsonObject();
            String id = string(object, "id");
            String name = string(object, "name");
            if (id != null) groups.add(new ReplicationGroup(id, name));
        }
        return groups;
    }

    /**
     * {@code GET /object/user-secret-keys/{uid}/{namespace}} returns up to two keys; returns the first non-empty one.
     */
    static String parseExistingSecretKey(JsonObject object) {
        String key = string(object, "secret_key_1");
        if (key == null || key.trim().isEmpty()) key = string(object, "secret_key_2");
        if (key == null || key.trim().isEmpty()) return null;
        return key;
    }

    /**
     * {@code POST /object/user-secret-keys/{uid}} and {@code POST /object/users} return a freshly generated key.
     */
    static String parseNewSecretKey(JsonObject object) {
        String key = string(object, "secret_key");
        if (key == null || key.trim().isEmpty()) return null;
        return key;
    }

    private static JsonArray array(JsonObject root, String key) {
        if (root.has(key) && root.get(key).isJsonArray()) return root.getAsJsonArray(key);
        return new JsonArray();
    }

    private static String string(JsonObject object, String key) {
        if (object.has(key) && !object.get(key).isJsonNull() && object.get(key).isJsonPrimitive()) {
            return object.get(key).getAsString();
        }
        return null;
    }

    private static boolean bool(JsonObject object, String key) {
        String value = string(object, key);
        return Boolean.parseBoolean(value);
    }

    private static String firstNonNull(String a, String b) {
        return a != null ? a : b;
    }
}

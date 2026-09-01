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

import com.google.gson.JsonObject;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Client for the ECS Management REST API, covering the operations needed to reproduce a cluster's tenancy structure:
 * namespaces, object users, secret keys, replication groups and buckets.
 */
public class EcsManagementClient implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(EcsManagementClient.class);

    private static final String HEADER_AUTH_TOKEN = "X-SDS-AUTH-TOKEN";
    private static final int PAGE_SIZE = 100;

    private final ClusterEndpoint endpoint;
    private final String role;
    private final Client client;
    private final String baseUrl;
    private String authToken;

    public EcsManagementClient(ClusterEndpoint endpoint, String role) {
        this.endpoint = endpoint;
        this.role = role;
        this.baseUrl = endpoint.managementBaseUrl();
        this.client = createClient(endpoint);
        authenticate();
    }

    private static Client createClient(ClusterEndpoint endpoint) {
        ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getProperties().put(ClientConfig.PROPERTY_CONNECT_TIMEOUT, endpoint.getConnectTimeoutMs());
        clientConfig.getProperties().put(ClientConfig.PROPERTY_READ_TIMEOUT, endpoint.getReadTimeoutMs());
        if (endpoint.isTrustSelfSignedCertificates()) {
            clientConfig.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
                    new HTTPSProperties((hostname, session) -> true, trustAllContext()));
        }
        return Client.create(clientConfig);
    }

    private static SSLContext trustAllContext() {
        try {
            TrustManager trustAll = new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            };
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, new TrustManager[]{trustAll}, new SecureRandom());
            return context;
        } catch (Exception e) {
            throw new EcsManagementException("could not create permissive SSL context", e);
        }
    }

    private void authenticate() {
        log.info("authenticating with {} ECS management API at {}", role, baseUrl);
        ClientResponse response;
        try {
            response = client.resource(baseUrl + "/login")
                    .header("Authorization", basicAuthHeader(endpoint.getAdminUsername(), endpoint.getAdminPassword()))
                    .get(ClientResponse.class);
        } catch (ClientHandlerException e) {
            String hint = e.getCause() instanceof SSLException
                    ? " - if the cluster uses a self-signed certificate, enable the trust-self-signed option"
                    : " - check the IP address, port and network connectivity";
            throw new EcsManagementException(String.format("cannot reach the %s cluster management API at %s (%s)%s",
                    role, baseUrl, rootCause(e), hint), e);
        }
        try {
            if (response.getStatus() != 200) {
                throw new EcsManagementException(String.format(
                        "%s cluster authentication failed (HTTP %d) - check the endpoint, credentials and, for a "
                                + "self-signed certificate, the trust-self-signed option",
                        role, response.getStatus()), response.getStatus());
            }
            authToken = response.getHeaders().getFirst(HEADER_AUTH_TOKEN);
            if (authToken == null || authToken.isEmpty())
                throw new EcsManagementException("no " + HEADER_AUTH_TOKEN + " returned by " + role + " cluster login");
        } finally {
            response.close();
        }
        log.info("authenticated with {} cluster", role);
    }

    private static String basicAuthHeader(String username, String password) {
        String credentials = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    // ---------------------------------------------------------------- namespaces

    public List<EcsNamespace> listNamespaces() {
        List<EcsNamespace> namespaces = new ArrayList<>();
        String marker = null;
        do {
            JsonObject page = getJson("/object/namespaces", marker, null);
            namespaces.addAll(ManagementResponseParser.parseNamespaces(page));
            marker = ManagementResponseParser.nextMarker(page);
        } while (marker != null);
        log.debug("{} cluster has {} namespaces", role, namespaces.size());
        return namespaces;
    }

    /**
     * @return the namespace, or null if it does not exist
     */
    public EcsNamespace getNamespace(String namespace) {
        JsonObject object = getJsonOrNull("/object/namespaces/namespace/" + encode(namespace), null);
        if (object == null) return null;
        return ManagementResponseParser.parseNamespaceDetail(object);
    }

    public void createNamespace(String namespace, String replicationGroupId) {
        JsonObject request = new JsonObject();
        request.addProperty("namespace", namespace);
        if (replicationGroupId != null) {
            request.addProperty("default_data_services_vpool", replicationGroupId);
        }
        post("/object/namespaces/namespace", request, "create namespace " + namespace);
        log.info("created namespace {} on {} cluster", namespace, role);
    }

    // ---------------------------------------------------------------- users

    public List<EcsObjectUser> listObjectUsers(String namespace) {
        List<EcsObjectUser> users = new ArrayList<>();
        String marker = null;
        do {
            JsonObject page = getJson("/object/users", marker, namespace);
            users.addAll(ManagementResponseParser.parseObjectUsers(page));
            marker = ManagementResponseParser.nextMarker(page);
        } while (marker != null);
        return users;
    }

    /**
     * Creates an object user. ECS returns a secret key from this call only in some versions, so callers should use
     * {@link #getOrCreateSecretKey(String, String)} afterwards rather than relying on the return value.
     *
     * @return the secret key if the API returned one, otherwise null
     */
    public String createObjectUser(String userId, String namespace) {
        JsonObject request = new JsonObject();
        request.addProperty("user", userId);
        request.addProperty("namespace", namespace);
        JsonObject response = post("/object/users", request, "create user " + userId);
        log.info("created object user {} in namespace {} on {} cluster", userId, namespace, role);
        return response == null ? null : ManagementResponseParser.parseNewSecretKey(response);
    }

    /**
     * Returns an existing secret key for the user, generating one if the user has none. The management API does not
     * expose an existing key in user listings, so this is the only way to obtain usable S3 credentials.
     *
     * @param allowGenerate when false, throws instead of generating a new key
     */
    public String getOrCreateSecretKey(String userId, String namespace, boolean allowGenerate) {
        JsonObject existing = getJsonOrNull(
                "/object/user-secret-keys/" + encode(userId) + "/" + encode(namespace), null);
        if (existing != null) {
            String key = ManagementResponseParser.parseExistingSecretKey(existing);
            if (key != null) {
                log.debug("using existing secret key for {} in namespace {} on {} cluster", userId, namespace, role);
                return key;
            }
        }

        if (!allowGenerate) {
            throw new EcsManagementException(String.format(
                    "user %s in namespace %s has no secret key on the %s cluster, and key generation is disabled",
                    userId, namespace, role));
        }

        JsonObject request = new JsonObject();
        request.addProperty("namespace", namespace);
        JsonObject response = post("/object/user-secret-keys/" + encode(userId), request,
                "generate secret key for " + userId);
        String key = response == null ? null : ManagementResponseParser.parseNewSecretKey(response);
        if (key == null)
            throw new EcsManagementException("no secret key returned when generating a key for " + userId);
        log.info("generated a new secret key for {} in namespace {} on {} cluster", userId, namespace, role);
        return key;
    }

    public String getOrCreateSecretKey(String userId, String namespace) {
        return getOrCreateSecretKey(userId, namespace, true);
    }

    // ---------------------------------------------------------------- replication groups

    public List<ReplicationGroup> listReplicationGroups() {
        JsonObject root = getJson("/vdc/data-service/vpools", null, null);
        return ManagementResponseParser.parseReplicationGroups(root);
    }

    // ---------------------------------------------------------------- buckets

    public List<EcsBucketInfo> listBuckets(String namespace) {
        List<EcsBucketInfo> buckets = new ArrayList<>();
        String marker = null;
        do {
            JsonObject page = getJson("/object/bucket", marker, namespace);
            buckets.addAll(ManagementResponseParser.parseBuckets(page));
            marker = ManagementResponseParser.nextMarker(page);
        } while (marker != null);
        return buckets;
    }

    /**
     * @return bucket details, or null if the bucket does not exist
     */
    public EcsBucketInfo getBucketInfo(String bucket, String namespace) {
        JsonObject object = getJsonOrNull("/object/bucket/" + encode(bucket) + "/info", namespace);
        if (object == null) return null;
        return ManagementResponseParser.parseBucketDetail(object);
    }

    /**
     * Creates a bucket owned by {@code owner}. Setting the owner at creation time is what makes the bucket belong to
     * the replicated object user rather than to the management admin.
     */
    public void createBucket(String bucket, String namespace, String replicationGroupId, String owner,
                             boolean fileSystemEnabled) {
        JsonObject request = new JsonObject();
        request.addProperty("name", bucket);
        request.addProperty("namespace", namespace);
        if (replicationGroupId != null) request.addProperty("vpool", replicationGroupId);
        if (owner != null) request.addProperty("owner", owner);
        if (fileSystemEnabled) request.addProperty("filesystem_enabled", true);
        post("/object/bucket", request, "create bucket " + bucket);
        log.info("created bucket {} in namespace {} owned by {} on {} cluster", bucket, namespace, owner, role);
    }

    public void setBucketOwner(String bucket, String namespace, String owner) {
        JsonObject request = new JsonObject();
        request.addProperty("new_owner", owner);
        if (namespace != null) request.addProperty("namespace", namespace);
        WebResource resource = client.resource(baseUrl + "/object/bucket/" + encode(bucket) + "/owner");
        ClientResponse response = resource
                .header(HEADER_AUTH_TOKEN, authToken)
                .type(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .post(ClientResponse.class, request.toString());
        readAndCheck(response, "set owner of bucket " + bucket);
        log.info("set owner of bucket {} to {} on {} cluster", bucket, owner, role);
    }

    // ---------------------------------------------------------------- transport

    private JsonObject getJson(String path, String marker, String namespace) {
        JsonObject object = doGet(path, marker, namespace, false);
        if (object == null) throw new EcsManagementException("empty response from " + path);
        return object;
    }

    private JsonObject getJsonOrNull(String path, String namespace) {
        return doGet(path, null, namespace, true);
    }

    private JsonObject doGet(String path, String marker, String namespace, boolean nullOnNotFound) {
        WebResource resource = client.resource(baseUrl + path);
        if (namespace != null) resource = resource.queryParam("namespace", namespace);
        if (marker != null) {
            resource = resource.queryParam("marker", marker).queryParam("limit", String.valueOf(PAGE_SIZE));
        }
        ClientResponse response = resource
                .header(HEADER_AUTH_TOKEN, authToken)
                .accept(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);
        try {
            // ECS is inconsistent about which of these it returns for a missing resource
            if (nullOnNotFound && (response.getStatus() == 404 || response.getStatus() == 400)) return null;
            String body = readAndCheck(response, "GET " + path);
            if (body == null || body.trim().isEmpty()) return null;
            return ManagementResponseParser.toObject(body);
        } finally {
            response.close();
        }
    }

    private JsonObject post(String path, JsonObject request, String description) {
        ClientResponse response = client.resource(baseUrl + path)
                .header(HEADER_AUTH_TOKEN, authToken)
                .type(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .post(ClientResponse.class, request.toString());
        try {
            String body = readAndCheck(response, description);
            if (body == null || body.trim().isEmpty()) return null;
            return ManagementResponseParser.toObject(body);
        } finally {
            response.close();
        }
    }

    private String readAndCheck(ClientResponse response, String description) {
        String body = response.hasEntity() ? response.getEntity(String.class) : null;
        if (response.getStatus() < 200 || response.getStatus() >= 300) {
            throw new EcsManagementException(String.format("failed to %s on %s cluster (HTTP %d): %s",
                    description, role, response.getStatus(), body), response.getStatus());
        }
        return body;
    }

    private static String rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) cause = cause.getCause();
        return cause.toString();
    }

    private static String encode(String value) {
        // ECS identifiers are restricted to characters that are safe in a path segment; guard against the obvious
        if (value == null) throw new EcsManagementException("path segment cannot be null");
        if (value.contains("/") || value.contains("?") || value.contains("#"))
            throw new EcsManagementException("illegal character in identifier: " + value);
        return value;
    }

    public String getRole() {
        return role;
    }

    @Override
    public void close() {
        client.destroy();
    }
}

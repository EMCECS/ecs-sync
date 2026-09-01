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
import com.emc.ecs.sync.config.Protocol;

import javax.xml.bind.annotation.XmlType;
import java.util.Arrays;

/**
 * Connection details for one ECS cluster: the management API (used to enumerate and create namespaces, users and
 * buckets) and the S3 data path (used to copy objects).
 */
@XmlType(name = "clusterEndpoint")
public class ClusterEndpoint {
    public static final int DEFAULT_MANAGEMENT_PORT = 4443;
    public static final int DEFAULT_DATA_PORT = 9020;
    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 30000;
    public static final int DEFAULT_READ_TIMEOUT_MS = 60000;

    private Protocol managementProtocol = Protocol.https;
    private String managementEndpoint;
    private int managementPort = DEFAULT_MANAGEMENT_PORT;
    private String adminUsername;
    private String adminPassword;
    private Protocol dataProtocol = Protocol.http;
    private String[] dataEndpoints;
    private int dataPort = DEFAULT_DATA_PORT;
    private boolean smartClientEnabled = true;
    private boolean trustSelfSignedCertificates;
    private int connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
    private int readTimeoutMs = DEFAULT_READ_TIMEOUT_MS;

    public void validate(String role) {
        if (managementProtocol == null) throw new ConfigurationException(role + " management protocol is required");
        if (managementEndpoint == null || managementEndpoint.trim().isEmpty())
            throw new ConfigurationException(role + " management endpoint is required");
        if (adminUsername == null || adminUsername.trim().isEmpty())
            throw new ConfigurationException(role + " admin username is required");
        if (adminPassword == null || adminPassword.isEmpty())
            throw new ConfigurationException(role + " admin password is required");
        if (managementPort <= 0) throw new ConfigurationException(role + " management port must be positive");
        if (dataPort <= 0) throw new ConfigurationException(role + " data port must be positive");
    }

    /**
     * The hosts to use for S3 data traffic. Defaults to the management endpoint when not explicitly configured, so
     * that a cluster can be specified with nothing more than one IP address and admin credentials.
     */
    public String[] effectiveDataEndpoints() {
        if (dataEndpoints != null && dataEndpoints.length > 0) return dataEndpoints;
        return new String[]{managementEndpoint};
    }

    public String managementBaseUrl() {
        return String.format("%s://%s:%d", managementProtocol, managementEndpoint, managementPort);
    }

    public Protocol getManagementProtocol() {
        return managementProtocol;
    }

    public void setManagementProtocol(Protocol managementProtocol) {
        this.managementProtocol = managementProtocol;
    }

    public String getManagementEndpoint() {
        return managementEndpoint;
    }

    public void setManagementEndpoint(String managementEndpoint) {
        this.managementEndpoint = managementEndpoint;
    }

    public int getManagementPort() {
        return managementPort;
    }

    public void setManagementPort(int managementPort) {
        this.managementPort = managementPort;
    }

    public String getAdminUsername() {
        return adminUsername;
    }

    public void setAdminUsername(String adminUsername) {
        this.adminUsername = adminUsername;
    }

    public String getAdminPassword() {
        return adminPassword;
    }

    public void setAdminPassword(String adminPassword) {
        this.adminPassword = adminPassword;
    }

    public Protocol getDataProtocol() {
        return dataProtocol;
    }

    public void setDataProtocol(Protocol dataProtocol) {
        this.dataProtocol = dataProtocol;
    }

    public String[] getDataEndpoints() {
        return dataEndpoints;
    }

    public void setDataEndpoints(String[] dataEndpoints) {
        this.dataEndpoints = dataEndpoints;
    }

    public int getDataPort() {
        return dataPort;
    }

    public void setDataPort(int dataPort) {
        this.dataPort = dataPort;
    }

    public boolean isSmartClientEnabled() {
        return smartClientEnabled;
    }

    public void setSmartClientEnabled(boolean smartClientEnabled) {
        this.smartClientEnabled = smartClientEnabled;
    }

    /**
     * ECS management endpoints commonly present a self-signed certificate. Enabling this disables certificate and
     * hostname verification for management API calls only.
     */
    public boolean isTrustSelfSignedCertificates() {
        return trustSelfSignedCertificates;
    }

    public void setTrustSelfSignedCertificates(boolean trustSelfSignedCertificates) {
        this.trustSelfSignedCertificates = trustSelfSignedCertificates;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public void setReadTimeoutMs(int readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }

    @Override
    public String toString() {
        return "ClusterEndpoint{" + managementBaseUrl() + ", dataEndpoints=" +
                Arrays.toString(effectiveDataEndpoints()) + ":" + dataPort + "}";
    }
}

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
package sync.ui.migration.e2e

import com.emc.ecs.sync.cluster.ClusterEndpoint
import com.emc.ecs.sync.config.Protocol
import sync.ui.storage.EcsStorage

/**
 * Adapts the UI's {@link EcsStorage} entry to the {@link ClusterEndpoint} used by the shared management client.
 */
class ClusterEndpoints {
    static final int DEFAULT_HTTP_DATA_PORT = 9020
    static final int DEFAULT_HTTPS_DATA_PORT = 9021

    static ClusterEndpoint from(EcsStorage storage, boolean trustSelfSigned) {
        def mgmt = new URI(storage.managementEndpoint)
        def data = new URI(storage.s3ApiEndpoint)

        def endpoint = new ClusterEndpoint()
        endpoint.managementProtocol = Protocol.valueOf(mgmt.scheme.toLowerCase())
        endpoint.managementEndpoint = mgmt.host
        if (mgmt.port > 0) endpoint.managementPort = mgmt.port
        endpoint.adminUsername = storage.sysAdminUser
        endpoint.adminPassword = storage.sysAdminPassword
        endpoint.trustSelfSignedCertificates = trustSelfSigned

        endpoint.dataProtocol = Protocol.valueOf(data.scheme.toLowerCase())
        endpoint.dataEndpoints = [data.host] as String[]
        endpoint.dataPort = data.port > 0 ? data.port
                : (endpoint.dataProtocol == Protocol.https ? DEFAULT_HTTPS_DATA_PORT : DEFAULT_HTTP_DATA_PORT)
        endpoint.smartClientEnabled = storage.s3SmartClient
        endpoint
    }

    /**
     * True when both storage entries address the same cluster management endpoint, i.e. a storage pool migration.
     */
    static boolean sameCluster(EcsStorage source, EcsStorage target) {
        def s = new URI(source.managementEndpoint), t = new URI(target.managementEndpoint)
        s.host == t.host && s.port == t.port
    }
}

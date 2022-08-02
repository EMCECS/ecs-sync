/*
 * Copyright (c) 2018-2019 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package sync.ui.storage

import com.dellemc.ecs.mgmt.MgmtApi
import com.dellemc.ecs.mgmt.MgmtClientConfig
import com.dellemc.ecs.mgmt.MgmtJerseyClient
import com.dellemc.model.user.UserSecretKeyCreateParam

class EcsService {
    private clientMap = [:]

    def testConfig(EcsStorage ecsStorage) {
        try {
            // check mgmt access
            getClient(ecsStorage).login()
            // check S3 API
            HttpURLConnection con = ecsStorage.s3ApiEndpoint.toURL().openConnection() as HttpURLConnection
            if (con.responseCode != 403)
                throw new RuntimeException("S3 API is not valid (expected 403 response, but got ${con.responseCode})")
            if (!con.errorStream.getText("UTF-8").find(/<Code>AccessDenied<\/Code>/))
                throw new RuntimeException("S3 API is not valid (expected AccessDenied error)")
            ''
        } catch (e) {
            return e.getMessage()
        }
    }

    def listReplicationGroups(EcsStorage ecsStorage) {
        getClient(ecsStorage).getReplicationGroups()
    }

    def listNamespaces(EcsStorage ecsStorage) {
        def namespaces = [], resp = getClient(ecsStorage).getNamespaces()
        while (true) {
            namespaces += resp.namespaces
            if (resp.nextMarker) resp = getClient(ecsStorage).getNamespaces(0, resp.nextMarker, null)
            else break
        }
        namespaces
    }

    def listNamespaces(EcsStorage ecsStorage, limit, marker, name) {
        getClient(ecsStorage).getNamespaces(limit, marker, name)
    }

    def createNamespace(EcsStorage ecsStorage, String namespace, String replicationGroup) {
        getClient(ecsStorage).createNamespace(namespace, null, replicationGroup)
    }

    def getNamespaceInfo(EcsStorage ecsStorage, String namespace) {
        getClient(ecsStorage).getNamespace(namespace)
    }

    def createObjectUser(EcsStorage ecsStorage, String username, String namespace) {
        getClient(ecsStorage).createObjectUser(username, namespace)
    }

    def getSecretKey(EcsStorage ecsStorage, String username, String namespace) {
        getClient(ecsStorage).getUserSecretKeys(username, namespace)
    }

    def setSecretKey(EcsStorage ecsStorage, String username, String namespace, String secretKey) {
        getClient(ecsStorage).createUserSecretKey(username,
                new UserSecretKeyCreateParam([namespace: namespace, secretKey: secretKey]))
    }

    def getObjectUserInfo(EcsStorage ecsStorage, String username, String namespace) {
        getClient(ecsStorage).getObjectUserInfo(username, namespace)
    }

    private synchronized MgmtApi getClient(EcsStorage ecsStorage) {
        def client = clientMap[ecsStorage]
        if (!client) {
            def endpoint = ecsStorage.managementEndpoint.toURL()
            def config = new MgmtClientConfig(endpoint.host, ecsStorage.sysAdminUser, ecsStorage.sysAdminPassword)
            if (endpoint.port > 0) config.port = endpoint.port
            config.protocol = endpoint.protocol.toLowerCase()
            client = new MgmtJerseyClient(config)
            clientMap[ecsStorage] = client
        }
        return client
    }
}

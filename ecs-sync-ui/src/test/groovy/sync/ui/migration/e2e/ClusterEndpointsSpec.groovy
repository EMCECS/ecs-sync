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

import com.emc.ecs.sync.config.Protocol
import spock.lang.Specification
import sync.ui.storage.EcsStorage

class ClusterEndpointsSpec extends Specification {

    private static EcsStorage storage(String mgmt, String s3, boolean smart = true) {
        new EcsStorage(managementEndpoint: mgmt, s3ApiEndpoint: s3, sysAdminUser: 'root',
                sysAdminPassword: 'pw', s3SmartClient: smart)
    }

    void "management endpoint is split into protocol, host and port"() {
        when:
        def endpoint = ClusterEndpoints.from(storage('https://10.0.0.1:4443', 'http://10.0.0.1:9020'), true)

        then:
        endpoint.managementProtocol == Protocol.https
        endpoint.managementEndpoint == '10.0.0.1'
        endpoint.managementPort == 4443
        endpoint.adminUsername == 'root'
        endpoint.adminPassword == 'pw'
        endpoint.trustSelfSignedCertificates
    }

    void "management port defaults when not present in the URL"() {
        when:
        def endpoint = ClusterEndpoints.from(storage('https://ecs.example.com', 'http://ecs.example.com:9020'), false)

        then:
        endpoint.managementPort == 4443
        !endpoint.trustSelfSignedCertificates
    }

    void "data endpoint is taken from the S3 API URL"() {
        when:
        def endpoint = ClusterEndpoints.from(storage('https://10.0.0.1:4443', 'http://10.0.0.9:9020'), true)

        then:
        endpoint.dataProtocol == Protocol.http
        endpoint.dataEndpoints == ['10.0.0.9'] as String[]
        endpoint.dataPort == 9020
    }

    void "data port defaults per protocol"() {
        expect:
        ClusterEndpoints.from(storage('https://h:4443', "${scheme}://h"), true).dataPort == expectedPort

        where:
        scheme  | expectedPort
        'http'  | 9020
        'https' | 9021
    }

    void "smart client flag is carried over"() {
        expect:
        ClusterEndpoints.from(storage('https://h:4443', 'http://h:9020', smart), true).smartClientEnabled == smart

        where:
        smart << [true, false]
    }

    void "same cluster is detected by management host and port"() {
        expect:
        ClusterEndpoints.sameCluster(storage(a, 'http://h:9020'), storage(b, 'http://h:9020')) == same

        where:
        a                       | b                       | same
        'https://10.0.0.1:4443' | 'https://10.0.0.1:4443' | true
        'https://10.0.0.1:4443' | 'https://10.0.0.2:4443' | false
        'https://10.0.0.1:4443' | 'https://10.0.0.1:9443' | false
        // a storage pool migration points both entries at the same cluster but different S3 endpoints
        'https://10.0.0.1:4443' | 'https://10.0.0.1:4443' | true
    }
}

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

import spock.lang.Specification
import sync.ui.migration.AbstractMigrationConfig
import sync.ui.migration.MigrationEntry

import javax.xml.bind.JAXBContext

class E2EMigrationConfigSpec extends Specification {

    void "defaults match the cluster-sync CLI"() {
        when:
        def config = new E2EMigrationConfig()

        then:
        config.sourceStorageType == 'ecs'
        config.targetStorageType == 'ecs'
        config.targetNamespaceSuffix == '_target'
        config.threadCount == 16
        config.verify
        config.includeVersions
        config.generateSourceSecretKeys
        config.trustSelfSignedCertificates
        config.selectedNamespaces == []
    }

    void "MigrationEntry creates an e2e config for the e2e type"() {
        when:
        def config = MigrationEntry.newMigrationConfig('e2e', 'some-guid')

        then:
        config instanceof E2EMigrationConfig
        config.guid == 'some-guid'
    }

    void "unknown migration types are still rejected"() {
        when:
        MigrationEntry.newMigrationConfig('nope', 'g')

        then:
        thrown(IllegalArgumentException)
    }

    void "config survives an XML round trip"() {
        given: "a context built from the abstract type, as the config store does"
        def context = JAXBContext.newInstance(AbstractMigrationConfig)
        def config = new E2EMigrationConfig(
                guid: 'guid-1',
                sourceXmlKey: 'storage/src-ecs.xml',
                targetXmlKey: 'storage/tgt-ecs.xml',
                selectedNamespaces: ['ns1', 'ns2'],
                replicationGroup: 'rg-gold',
                targetNamespaceSuffix: '_pool2',
                threadCount: 32,
                verify: false,
                includeVersions: false,
                generateSourceSecretKeys: false,
                trustSelfSignedCertificates: false)

        when: "marshalled"
        def writer = new StringWriter()
        context.createMarshaller().marshal(config, writer)
        def xml = writer.toString()

        then: "every configured field is present"
        xml.contains('<selectedNamespaces>ns1</selectedNamespaces>')
        xml.contains('<selectedNamespaces>ns2</selectedNamespaces>')
        xml.contains('<replicationGroup>rg-gold</replicationGroup>')
        xml.contains('<targetNamespaceSuffix>_pool2</targetNamespaceSuffix>')
        xml.contains('<threadCount>32</threadCount>')

        when: "unmarshalled back through the abstract type"
        def parsed = context.createUnmarshaller().unmarshal(new StringReader(xml))

        then: "the concrete subtype is restored with its values"
        parsed instanceof E2EMigrationConfig
        parsed.guid == 'guid-1'
        parsed.sourceXmlKey == 'storage/src-ecs.xml'
        parsed.targetXmlKey == 'storage/tgt-ecs.xml'
        parsed.selectedNamespaces == ['ns1', 'ns2']
        parsed.replicationGroup == 'rg-gold'
        parsed.targetNamespaceSuffix == '_pool2'
        parsed.threadCount == 32
        !parsed.verify
        !parsed.includeVersions
        !parsed.generateSourceSecretKeys
        !parsed.trustSelfSignedCertificates
    }
}

/*
 * Copyright (c) 2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package sync.ui

import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.config.storage.TestConfig
import com.emc.ecs.sync.rest.SyncProgress
import grails.gorm.transactions.Rollback
import grails.testing.mixin.integration.Integration
import groovy.util.logging.Slf4j
import spock.lang.Specification

import javax.xml.bind.JAXBContext

@Integration
@Rollback
@Slf4j
class JaxbSpec extends Specification {
    def jaxbContext = JAXBContext.newInstance(SyncHttpMessageConverter.jaxbClasses)

    def syncConfig = new SyncConfig(
            source: new TestConfig(),
            target: new TestConfig(),
    )

    def setup() {
    }

    def cleanup() {
    }

    void "Test UiConfig XML"() {
        when: "UiConfig is marshalled"
        def uiConfig = new UiConfig(alertEmail: 'me@here.com')
        def writer = new StringWriter()
        jaxbContext.createMarshaller().marshal(uiConfig, writer)
        def xml = writer.toString()
        log.info("generated XML: {}", xml)

        then: "XML is generated"
        xml.contains("<uiConfig")

        when: "UiConfig is unmarshalled"
        def uiConfig2 = jaxbContext.createUnmarshaller().unmarshal(new StringReader(xml))

        then: "Unmarshalled object is the same"
        uiConfig == uiConfig2
    }

    void "Test UiConfig 3.4 Compatibility"() {
        when: "3.4 UiConfig is read"
        def oldUiConfigXml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
                '<uiConfig xmlns:ns2="http://www.emc.com/ecs/sync/model">' +
                '<configStorageType>LocalDisk</configStorageType>' +
                '<filePath>/opt/emc/ecs-sync/config</filePath>' +
                '<protocol>HTTP</protocol>' +
                '<port>9020</port>' +
                '<smartClient>true</smartClient>' +
                '<configBucket>ecs-sync</configBucket>' +
                '<autoArchive>false</autoArchive>' +
                '<alertEmail>me@here.com</alertEmail>' +
                '</uiConfig>'
        UiConfig uiConfig = (UiConfig) jaxbContext.createUnmarshaller().unmarshal(new StringReader(oldUiConfigXml))

        then: "Read is successful"
        uiConfig.alertEmail == "me@here.com"
    }

    void "Test ScheduledSync XML"() {
        when: "ScheduledSync is marshalled"
        def scheduledSync = new ScheduledSync(
                daysOfWeek: [ScheduledSync.Day.Monday, ScheduledSync.Day.Wednesday, ScheduledSync.Day.Friday],
                startHour: 5,
                startMinute: 30,
                config: syncConfig
        )
        def writer = new StringWriter()
        jaxbContext.createMarshaller().marshal(scheduledSync, writer)
        def xml = writer.toString()
        log.info("generated XML: {}", xml)

        then: "XML is generated"
        xml.contains("xmlns:ns2=\"http://www.emc.com/ecs/sync/model\"")
        xml.contains("<scheduledSync")
        xml.contains("<daysOfWeek")
        xml.contains("<config>")
        xml.contains("<ns2:options>")
        xml.contains("<ns2:source>")
        xml.contains("<ns2:target>")

        when: "ScheduledSync is unmarshalled"
        def scheduledSync2 = jaxbContext.createUnmarshaller().unmarshal(new StringReader(xml))

        then: "Unmarshalled object is the same"
        scheduledSync == scheduledSync2
    }

    void "Test SyncResult XML"() {
        when: "SyncResult is marshalled"
        def syncResult = new SyncResult(
                config: syncConfig,
                progress: new SyncProgress()
        )
        def writer = new StringWriter()
        jaxbContext.createMarshaller().marshal(syncResult, writer)
        def xml = writer.toString()
        log.info("generated XML: {}", xml)

        then: "XML is generated"
        xml.contains("<syncResult xmlns:ns2=\"http://www.emc.com/ecs/sync/model\">")
        xml.contains("<config>")
        xml.contains("<ns2:options>")
        xml.contains("<ns2:source>")
        xml.contains("<ns2:target>")

        when: "SyncResult is unmarshalled"
        def syncResult2 = jaxbContext.createUnmarshaller().unmarshal(new StringReader(xml))

        then: "Unmarshalled object is the same"
        syncResult == syncResult2
    }
}

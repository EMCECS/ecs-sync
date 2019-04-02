/*
 * Copyright 2013-2018 EMC Corporation. All Rights Reserved.
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
package sync.ui.migration

import grails.validation.Validateable
import groovy.util.logging.Slf4j
import sync.ui.config.ConfigService
import sync.ui.migration.a2e.A2EMigrationConfig

import java.util.regex.Pattern

@Slf4j
class MigrationEntry implements Validateable {
    static String basePrefix = "migration/"
    static Pattern keyPattern = Pattern.compile('/([^/-]+)-([^/]+)\\.xml$')

    static List<MigrationEntry> list(ConfigService configService) {
        configService.listConfigObjects(basePrefix).collectMany {
            try {
                if (it.find(keyPattern)) [new MigrationEntry([configService: configService, xmlKey: it])]
                else []
            } catch (e) {
                log.warn "could not load migration entry: ${it}", e
                [] // if we can't read it, skip it
            }
        } // should be in alphabetical order from bucket list
    }

    static AbstractMigrationConfig newMigrationConfig(type, guid) {
        // TODO: abstract these types better
        if (type == "a2e") return new A2EMigrationConfig([guid: guid])
        else throw new IllegalArgumentException("Migration type ${type} is unknown")
    }

    ConfigService configService

    String type
    String guid = UUID.randomUUID().toString()
    @Lazy
    volatile String prefix = "${basePrefix}${type}-${guid}"
    @Lazy
    volatile String xmlKey = "${prefix}.xml"
    @Lazy
    volatile allKeys = [xmlKey]
    @Lazy(soft = true)
    volatile AbstractMigrationConfig migrationConfig = exists() ? configService.readConfigObject(xmlKey, AbstractMigrationConfig.class) : newMigrationConfig(type, guid)

    boolean exists() {
        return (type && configService && configService.configObjectExists(xmlKey))
    }

    def write() {
        configService.writeConfigObject(xmlKey, migrationConfig, 'application/xml')
    }

    def setXmlKey(String key) {
        def keyMatch = keyPattern.matcher(key)
        if (keyMatch.find()) {
            this.type = keyMatch.group(1)
            this.guid = keyMatch.group(2)
        } else {
            throw new RuntimeException("could not parse MigrationEntry key: ${key}")
        }
    }

    static constraints = {
        configService nullable: true
        type blank: false
        migrationConfig validator: { it.validate() ?: 'invalid' }
    }
}

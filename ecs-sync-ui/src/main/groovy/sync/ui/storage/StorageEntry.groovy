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
package sync.ui.storage

import grails.validation.Validateable
import groovy.util.logging.Slf4j
import sync.ui.config.ConfigService

import java.util.regex.Pattern

@Slf4j
class StorageEntry implements Validateable {
    static String prefix = "storage/"
    static Pattern keyPattern = Pattern.compile('/([^/]+)-([^/-]+)\\.xml$')

    static List<StorageEntry> list(ConfigService configService) {
        configService.listConfigObjects(prefix).collectMany {
            try {
                [new StorageEntry([configService: configService, xmlKey: it])]
            } catch (e) {
                log.warn "could not load storage cluster entry: ${it}", e
                [] // if we can't read it, skip it
            }
        }.sort { a, b -> a.name <=> b.name } // alphabetical order
    }

    static AbstractStorage newStorage(type) {
        // TODO: abstract these types better
        if (type == "atmos") return new AtmosStorage()
        else if (type == "ecs") return new EcsStorage()
        else throw new IllegalArgumentException("Type ${type} is unknown")
    }

    ConfigService configService

    String name
    String type
    @Lazy
    volatile String xmlKey = "${prefix}${name}-${type}.xml"
    @Lazy
    volatile allKeys = [xmlKey]
    @Lazy(soft = true)
    volatile AbstractStorage storage = exists() ? configService.readConfigObject(xmlKey, AbstractStorage.class) : newStorage(type)

    boolean exists() {
        return (name && configService && configService.configObjectExists(xmlKey))
    }

    def write() {
        configService.writeConfigObject(xmlKey, storage, 'application/xml')
    }

    def setXmlKey(String key) {
        def keyMatch = keyPattern.matcher(key)
        if (keyMatch.find()) {
            name = keyMatch.group(1)
            type = keyMatch.group(2)
        } else {
            throw new RuntimeException("could not parse StorageEntry key: ${key}")
        }
    }

    static constraints = {
        configService nullable: true
        name blank: false
        storage validator: { it.validate() ?: 'invalid' }
    }
}

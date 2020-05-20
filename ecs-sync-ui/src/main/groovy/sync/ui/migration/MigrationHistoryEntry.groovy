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

import groovy.util.logging.Slf4j
import sync.ui.config.ConfigService

@Slf4j
class MigrationHistoryEntry {
    static String prefix = "archive/migration/"
    static String idFormat = "yyyyMMdd'T'HHmmss"

    static List<MigrationHistoryEntry> list(ConfigService configService) {
        configService.listConfigObjects(prefix).collectMany {
            try {
                [new MigrationHistoryEntry([configService: configService, xmlKey: it])]
            } catch (e) {
                log.warn "could not load migration history entry: ${it}", e
                [] // if we can't read it, skip it
            }
        }.sort { a, b -> b.startTime <=> a.startTime } // reverse-chronological order
    }

    ConfigService configService

    String id
    String name
    Date startTime
    @Lazy
    volatile String xmlKey = "${prefix}${id}.xml"
    @Lazy
    volatile String reportKey = "${prefix}report/${id}.report.csv"
    @Lazy
    volatile String reportFileName = fileName(reportKey)
    @Lazy
    volatile allKeys = [xmlKey, reportKey]
    @Lazy(soft = true)
    URI reportUri = configService.configObjectQuickLink(reportKey)
    @Lazy(soft = true)
    volatile MigrationResult migrationResult = configService.readConfigObject(xmlKey, MigrationResult.class)

    def write() {
        configService.writeConfigObject(xmlKey, migrationResult, 'application/xml')
    }

    boolean getReportExists() {
        return (id && configService.configObjectExists(reportKey))
    }

    def setId(String id) {
        this.id = id
        def tokens = id.tokenize('-')
        this.name = tokens.size() > 1 ? tokens[1..-1].join('-') : null
        this.startTime = Date.parse(idFormat, tokens[0])
    }

    def setName(String name) {
        this.name = name
        inferId()
    }

    def setStartTime(Date startTime) {
        this.startTime = startTime
        inferId()
    }

    def setXmlKey(String key) {
        setId(fileName(key).replaceFirst(/[.]xml$/, ''))
    }

    private inferId() {
        if (startTime) {
            if (name) this.id = "${startTime.format(idFormat)}-${name}"
            else this.id = startTime.format(idFormat)
        }
    }

    private static String fileName(String key) {
        return key.split('[/\\\\]').last()
    }
}

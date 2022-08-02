/*
 * Copyright (c) 2016-2020 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import groovy.util.logging.Slf4j
import sync.ui.config.ConfigService

@Slf4j
class SyncHistoryEntry {
    static String prefix = "archive/"
    static String idFormat = "yyyyMMdd'T'HHmmss"

    static List<SyncHistoryEntry> list(ConfigService configService) {
        configService.listConfigObjects(prefix).collectMany {
            try {
                [new SyncHistoryEntry([configService: configService, xmlKey: it])]
            } catch (e) {
                log.warn "could not load sync history entry: ${it}", e
                [] // if we can't read it, skip it
            }
        }.sort { a, b -> b.startTime <=> a.startTime } // reverse-chronological order
    }

    ConfigService configService

    String id
    String jobName
    Date startTime
    @Lazy
    volatile String xmlKey = "${prefix}${id}.xml"
    @Lazy
    volatile String reportKey = "${prefix}report/${id}.report.csv"
    @Lazy
    volatile String reportFileName = fileName(reportKey)
    @Lazy
    volatile String errorsKey = "${prefix}errors/${id}.errors.csv"
    @Lazy
    volatile String errorsFileName = fileName(errorsKey)
    @Lazy
    volatile allKeys = [xmlKey, reportKey, errorsKey]
    @Lazy(soft = true)
    URI reportUri = configService.configObjectQuickLink(reportKey)
    @Lazy(soft = true)
    volatile URI errorsUri = configService.configObjectQuickLink(errorsKey)
    @Lazy(soft = true)
    volatile SyncResult syncResult = configService.readConfigObject(xmlKey, SyncResult.class)

    def write() {
        configService.writeConfigObject(xmlKey, syncResult, 'application/xml')
    }

    boolean getReportExists() {
        return (id && configService.configObjectExists(reportKey))
    }

    boolean getErrorsExists() {
        return (id && configService.configObjectExists(errorsKey))
    }

    def setId(String id) {
        this.id = id
        def tokens = id.tokenize('-')
        this.jobName = tokens.size() > 1 ? tokens[1..-1].join('-') : null
        this.startTime = Date.parse(idFormat, tokens[0])
    }

    def setJobName(String jobName) {
        this.jobName = jobName
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
            if (jobName) this.id = "${startTime.format(idFormat)}-${jobName}"
            else this.id = startTime.format(idFormat)
        }
    }

    private static String fileName(String key) {
        return key.split('[/\\\\]').last()
    }
}

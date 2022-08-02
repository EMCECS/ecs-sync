/*
 * Copyright (c) 2018 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package sync.ui.config

import grails.gorm.transactions.Transactional
import sync.ui.ConfigException
import sync.ui.UiConfig

@Transactional(readOnly = true)
abstract class ConfigService {
    abstract List<String> listConfigObjects(String path)

    abstract boolean configObjectExists(String path)

    abstract <T> T readConfigObject(String path, Class<T> resultType)

    abstract void writeConfigObject(String path, content, String contentType)

    abstract void deleteConfigObject(String path)

    abstract URI configObjectQuickLink(String path)

    abstract void writeConfig(UiConfig uiConfig)

    abstract void readConfig(UiConfig uiConfig)

    UiConfig readConfig(required = true) {
        def uiConfig = getConfig(required)
        readConfig(uiConfig)
        return uiConfig
    }

    static UiConfig getConfig(required = true) {
        UiConfig.withTransaction {
            def uiConfig = UiConfig.first([readOnly: true])
            if (uiConfig == null && required)
                throw new ConfigException("Missing configuration - please provide configuration details or read from an existing configuration")
            return uiConfig
        }
    }
}

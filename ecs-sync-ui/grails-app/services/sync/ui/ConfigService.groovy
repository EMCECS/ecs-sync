/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package sync.ui

import grails.transaction.Transactional

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

    UiConfig readConfig() {
        def uiConfig = getConfig()
        readConfig(uiConfig)
        return uiConfig
    }

    static UiConfig getConfig() {
        def uiConfig = UiConfig.first([readOnly: true])
        if (uiConfig == null) throw new ConfigException("Missing configuration")
        return uiConfig
    }
}

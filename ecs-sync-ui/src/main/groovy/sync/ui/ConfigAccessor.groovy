/*
 * Copyright (c) 2016-2018 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import sync.ui.config.ConfigService

trait ConfigAccessor {
    def ecsConfigService
    def fileConfigService

    ConfigService getConfigService() {
        getConfigService(ConfigService.getConfig())
    }

    ConfigService getConfigService(uiConfig) {
        if (uiConfig.configStorageType == ConfigStorageType.ECS) return ecsConfigService
        else if (uiConfig.configStorageType == ConfigStorageType.LocalDisk) return fileConfigService
    }
}

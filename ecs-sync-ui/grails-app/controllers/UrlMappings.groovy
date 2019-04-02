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
import sync.ui.ConfigException

class UrlMappings {
    static mappings = {
        "/download/$path**"(controller: 'download', action: 'get')

        "/$controller/$action?/$id?(.$format)?"{
            constraints {
                // apply constraints here
            }
        }

        "/"(controller: 'status')
        "500"(controller: 'uiConfig', action: 'index', exception: ConfigException)
        "500"(view: '/notRunning', exception: ConnectException)
        "500"(view: '/error')
        "404"(view: '/notFound')
    }
}

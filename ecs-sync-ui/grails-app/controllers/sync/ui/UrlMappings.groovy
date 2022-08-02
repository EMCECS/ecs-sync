/*
 * Copyright (c) 2015-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

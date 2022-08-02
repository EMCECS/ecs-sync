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

class DownloadController implements ConfigAccessor {
    static allowedMethods = [get: "GET"]

    def grailsMimeUtility

    def get(String path) {
        if (configService.configObjectExists(path)) {
            def file = new File(path)
            def filename = file.name
            response.setHeader('Content-Disposition', "attachment; filename=${filename}")
            if (filename.contains('.'))
                response.contentType = grailsMimeUtility.getMimeTypeForExtension(filename.substring(filename.lastIndexOf('.') + 1))
            response.outputStream << configService.readConfigObject(path, InputStream.class)
            response.outputStream.flush()
        } else {
            render status: 404
        }
    }
}
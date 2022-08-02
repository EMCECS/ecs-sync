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
package sync.ui

import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.rest.SyncProgress

class SyncJobService {
    def rest
    def jobServer

    def submitJob(SyncConfig syncConfig) {
        def response = rest.put("${jobServer}/job") {
            contentType 'application/xml'
            body syncConfig
        }
        [
                success   : response.statusCode.is2xxSuccessful(),
                statusCode: response.status,
                statusText: response.statusCode.reasonPhrase,
                message   : response.text,
                jobId     : response.headers.getFirst('x-emc-job-id')
        ]
    }

    def getJobStatus(jobId) {
        rest.get("${jobServer}/job/${jobId}/progress") { accept(SyncProgress.class) }.body as SyncProgress
    }

    def deleteJob(jobId, keepDatabase) {
        rest.delete("${jobServer}/job/${jobId}?keepDatabase=${keepDatabase}")
    }
}

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

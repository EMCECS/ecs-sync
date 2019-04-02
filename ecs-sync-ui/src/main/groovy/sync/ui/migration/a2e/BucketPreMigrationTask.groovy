package sync.ui.migration.a2e

import com.emc.object.s3.bean.EncodingType
import com.emc.object.s3.request.ListMultipartUploadsRequest
import groovy.util.logging.Slf4j
import sync.ui.migration.AbstractMigrationTask
import sync.ui.migration.TaskStatus

@Slf4j
class BucketPreMigrationTask extends AbstractMigrationTask<A2EMigration> {
    def sourceUser
    def bucket

    @Lazy
    volatile name = "Checking bucket ${migration.migrationConfig.atmosSubtenantName}/${bucket} before migration"
    @Lazy
    volatile description = "This will perform a few validation checks prior to migrating a bucket to avoid any " +
            "client/application issues. The following checks are performed:<br>" +
            "<ul><li>Check for incomplete MPUs. If any are found that are younger than the stale MPU threshold, return an error (this will pause the migration)</li></ul>"

    @Override
    void execute() {
        def s3UserClient = migration.sourceUserS3Client(sourceUser)

        // list all open MPUs
        def now = new Date()
        def listMpuRequest = new ListMultipartUploadsRequest(bucket).withEncodingType(EncodingType.url)
        def problemMpus = []
        int mpuCount = 0
        while (true) {
            def result = s3UserClient.listMultipartUploads(listMpuRequest)

            result.uploads.each {
                mpuCount++
                def age = now - it.initiated
                if (age.days < migration.migrationConfig.staleMpuThresholdDays) problemMpus.add(it)
            }

            if (!result.truncated) break
            listMpuRequest.keyMarker = result.nextKeyMarker
            listMpuRequest.uploadIdMarker = result.uploadIdMarker
        }

        if (problemMpus) {
            taskStatus = TaskStatus.Error
            def mpuSummary = problemMpus.collect {
                "key: ${it.key}, id: ${it.uploadId}, owner: ${it.owner.id}, initiated: ${it.initiated}"
            }.join('<br>')
            taskMessage = "${problemMpus.size()} / ${mpuCount} MPUs are younger than ${migration.migrationConfig.staleMpuThresholdDays} days. You should resume the migration after these are complete or verified to be abandoned<br>${mpuSummary}"
        } else {
            taskStatus = TaskStatus.Success
            if (mpuCount)
                taskMessage = "${mpuCount} MPUs found, all older than ${migration.migrationConfig.staleMpuThresholdDays} days and deemed stale"
            else
                taskMessage = "No MPUs found"
        }
        percentComplete = 100
    }
}

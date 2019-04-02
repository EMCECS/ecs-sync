package sync.ui.migration.a2e


import groovy.util.logging.Slf4j
import sync.ui.migration.AbstractMigrationTask
import sync.ui.migration.TaskStatus

@Slf4j
class CreateBucketTask extends AbstractMigrationTask<A2EMigration> {
    String atmosUser
    String bucket

    @Lazy
    volatile name = "Create bucket ${bucket} for user ${ecsUser}"
    def description = "Creates the specified bucket with default options if it does not exist."

    @Lazy
    volatile ecsUser = migration.migrationConfig.userMap[atmosUser]

    @Override
    void execute() {
        try {
            def s3Client = migration.targetUserS3Client(atmosUser)

            if (!s3Client.bucketExists(bucket)) {
                s3Client.createBucket(bucket)
                taskMessage = "Created bucket ${bucket} as user ${ecsUser}"
            } else {
                taskMessage = "Bucket ${bucket} already exists"
            }
            taskStatus = TaskStatus.Success
        } catch (e) {
            // unexpected error
            log.error("Unexpected error", e)
            taskStatus = TaskStatus.Error
            taskMessage = e.message
        }

        percentComplete = 100
    }
}

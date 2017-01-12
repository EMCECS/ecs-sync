package sync.ui

class ArchiveController implements ConfigAccessor {
    def index() {
        def archives = ArchiveEntry.list(configService)
        [archives: archives]
    }

    def delete() {
        new ArchiveEntry([id: params.archiveId]).allKeys.each { configService.deleteConfigObject(it) }
        redirect action: 'index'
    }
}

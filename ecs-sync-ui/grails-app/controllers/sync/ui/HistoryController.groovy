package sync.ui

class HistoryController implements ConfigAccessor {
    def index() {
        def entries = HistoryEntry.list(configService)
        [historyEntries: entries]
    }

    def delete() {
        new HistoryEntry([id: params.entryId]).allKeys.each { configService.deleteConfigObject(it) }
        redirect action: 'index'
    }
}

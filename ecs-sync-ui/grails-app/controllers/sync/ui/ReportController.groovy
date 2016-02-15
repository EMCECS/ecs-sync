package sync.ui

class ReportController {
    def ecsService

    def index() {
        def results = ResultEntry.list(ecsService)
        [results: results]
    }

    def delete() {
        new ResultEntry([id: params.resultId]).allKeys.each { ecsService.deleteConfigObject(it) }
        redirect action: 'index'
    }
}

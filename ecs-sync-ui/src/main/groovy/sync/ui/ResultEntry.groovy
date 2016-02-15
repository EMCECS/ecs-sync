package sync.ui

class ResultEntry {
    static String prefix = "results/"
    static String idFormat = "yyyyMMdd'T'HHmmss";

    static List<ResultEntry> list(EcsService ecsService) {
        ecsService.listConfigObjects(prefix).collect {
            new ResultEntry([ecsService: ecsService, xmlKey: it])
        }.sort { a, b -> b.startTime <=> a.startTime } // reverse-chronological order
    }

    EcsService ecsService

    String id
    int jobId
    Date startTime
    @Lazy
    String dbName = "sync_${startTime.format(idFormat)}_${jobId}"
    @Lazy
    String xmlKey = "${prefix}${id}.xml"
    @Lazy
    String reportKey = "${prefix}report/${id}.report.csv"
    @Lazy
    String reportFileName = fileName(reportKey)
    @Lazy
    String errorsKey = "${prefix}errors/${id}.errors.csv"
    @Lazy
    String errorsFileName = fileName(errorsKey)
    @Lazy
    def allKeys = [xmlKey, reportKey, errorsKey]
    @Lazy(soft = true)
    URL reportUrl = ecsService.configObjectQuickLink(reportKey)
    @Lazy(soft = true)
    URL errorsUrl = ecsService.configObjectQuickLink(errorsKey)
    @Lazy(soft = true)
    SyncResult syncResult = ecsService.readConfigObject(xmlKey, SyncResult.class)

    def write() {
        ecsService.writeConfigObject(xmlKey, syncResult, 'application/xml')
    }

    boolean getReportExists() {
        return (id && ecsService.configObjectExists(reportKey))
    }

    boolean getErrorsExists() {
        return (id && ecsService.configObjectExists(errorsKey))
    }

    def setId(String id) {
        this.id = id
        this.jobId = id.tokenize('-job')?.last()?.toInteger()
        this.startTime = Date.parse(idFormat, id.tokenize('-')?.first())
    }

    def setJobId(int jobId) {
        this.jobId = jobId
        inferId()
    }

    def setStartTime(Date startTime) {
        this.startTime = startTime
        inferId()
    }

    def setXmlKey(String key) {
        setId(fileName(key).replaceFirst(/[.]xml$/, ''))
    }

    private inferId() {
        if (startTime && jobId) this.id = "${startTime.format(idFormat)}-job${jobId}"
    }

    private static String fileName(String key) {
        return key.split('/').last()
    }
}

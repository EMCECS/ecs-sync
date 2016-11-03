package sync.ui

import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.rest.HostInfo
import com.emc.ecs.sync.rest.SyncProgress
import groovy.time.TimeCategory

class ReportService {
    def rest
    def jobServer
    def ecsService

    def generateReport(jobId) {
        SyncConfig sync = rest.get("${jobServer}/job/${jobId}") { accept(SyncConfig.class) }.body
        SyncProgress progress = rest.get("${jobServer}/job/${jobId}/progress") { accept(SyncProgress.class) }.body
        HostInfo host = rest.get("${jobServer}/host") { accept(HostInfo.class) }.body

        def sourcePath = SyncUtil.getSourcePath(sync)
        def targetPath = SyncUtil.getTargetPath(sync)
        Date startTime = new Date(progress.syncStartTime)

        // generate summary CSV
        long xputBytes = 0
        if (progress.bytesComplete && progress.runtimeMs)
            xputBytes = progress.bytesComplete * 1000 / progress.runtimeMs
        long xputFiles = 0
        if (progress.objectsComplete && progress.runtimeMs)
            xputFiles = progress.objectsComplete * 1000 / progress.runtimeMs
        double cpuUsage = 0.0
        if (progress.cpuTimeMs && progress.runtimeMs && host.hostCpuCount)
            cpuUsage = (double) progress.cpuTimeMs / (double) progress.runtimeMs / (double) host.hostCpuCount * 100
        def rows = [[]]
        rows << ['Source', "${sync.source.pluginClass.tokenize('.').last()}"]
        rows << ['Source Path', "${sourcePath}"]
        rows << ['Target', "${sync.target.pluginClass.tokenize('.').last()}"]
        rows << ['Target Path', "${targetPath}"]
        rows << ['Started At', "${startTime.format('yyyy-MM-dd hh:mm:ssa')}"]
        rows << ['Stopped At', "${progress.syncStopTime ? new Date(progress.syncStopTime).format('yyyy-MM-dd hh:mm:ssa') : 'N/A'}"]
        rows << ['Duration', "${DisplayUtil.shortDur(TimeCategory.minus(new Date(progress.runtimeMs), new Date(0)))}"]
        rows << ['Thread Count', "${sync.syncThreadCount}"]
        rows << ['Total Files and Directories Estimated', "${progress.totalObjectsExpected}"]
        rows << ['Total Files and Directories Processed', "${progress.objectsComplete}"]
        rows << ['Total Bytes Estimated', "${progress.totalBytesExpected ? DisplayUtil.simpleSize(progress.totalBytesExpected) + 'B' : 'N/A'}"]
        rows << ['Total Bytes Transferred', "${DisplayUtil.simpleSize(progress.bytesComplete)}B"]
        rows << ['General Error Message', "${progress.runError}"]
        rows << ['Total Errors', "${progress.objectsFailed}"]
        rows << ['Overall Throughput (files)', "${DisplayUtil.simpleSize(xputFiles)}/s"]
        rows << ['Overall Throughput (bytes)', "${DisplayUtil.simpleSize(xputBytes)}B/s"]
        rows << ['Overall CPU Usage', "${cpuUsage.trunc(1)}%"]

        def entry = new ResultEntry([ecsService: ecsService, jobId: jobId.toLong(), startTime: startTime])
        entry.syncResult = new SyncResult([config: sync, progress: progress])
        entry.write()
        ecsService.writeConfigObject(entry.reportKey, matrixToCsv(rows), 'text/csv')
        if (sync.dbFile || sync.dbTable) {
            def con = "${jobServer}/job/${jobId}/errors.csv".toURL().openConnection()
            ecsService.writeConfigObject(entry.errorsKey, con.inputStream, con.getHeaderField('Content-Type'))
        }
        entry
    }

    private static String matrixToCsv(rows) {
        def csv = ''
        rows.each { row ->
            row.eachWithIndex { col, i ->
                if (i > 0) csv += ','
                csv += '"' + col + '"'
            }
            csv += '\n'
        }
        return csv
    }
}

package sync.ui

import com.emc.ecs.sync.config.ConfigUtil
import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.rest.HostInfo
import com.emc.ecs.sync.rest.SyncProgress
import groovy.time.TimeCategory

class HistoryService implements ConfigAccessor {
    def rest
    def jobServer

    def archiveJob(jobId) {
        SyncConfig sync = rest.get("${jobServer}/job/${jobId}") { accept(SyncConfig.class) }.body as SyncConfig
        SyncProgress progress = rest.get("${jobServer}/job/${jobId}/progress") { accept(SyncProgress.class) }.body as SyncProgress
        HostInfo host = rest.get("${jobServer}/host") { accept(HostInfo.class) }.body as HostInfo

        def sourceName = ConfigUtil.wrapperFor(sync.source.getClass()).label
        def sourcePath = SyncUtil.getLocation(sync.source)
        def targetName = ConfigUtil.wrapperFor(sync.target.getClass()).label
        def targetPath = SyncUtil.getLocation(sync.target)
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
        rows << ['Source', "${sourceName}"]
        rows << ['Source Location', "${sourcePath}"]
        rows << ['Target', "${targetName}"]
        rows << ['Target Location', "${targetPath}"]
        rows << ['Started At', "${startTime.format('yyyy-MM-dd hh:mm:ssa')}"]
        rows << ['Stopped At', "${progress.syncStopTime ? new Date(progress.syncStopTime).format('yyyy-MM-dd hh:mm:ssa') : 'N/A'}"]
        rows << ['Duration', "${DisplayUtil.shortDur(TimeCategory.minus(new Date(progress.runtimeMs), new Date(0)))}"]
        rows << ['Thread Count', "${sync.options.threadCount}"]
        rows << ['Total Files and Directories Estimated', "${progress.totalObjectsExpected}"]
        rows << ['Total Files and Directories Processed', "${progress.objectsComplete}"]
        rows << ['Total Files and Directories Skipped', "${progress.objectsSkipped}"]
        rows << ['Total Errors', "${progress.objectsFailed}"]
        rows << ['Total Bytes Estimated', "${progress.totalBytesExpected ? DisplayUtil.simpleSize(progress.totalBytesExpected) + 'B' : 'N/A'}"]
        rows << ['Total Bytes Transferred', "${DisplayUtil.simpleSize(progress.bytesComplete)}B"]
        rows << ['Total Bytes Skipped', "${DisplayUtil.simpleSize(progress.bytesSkipped)}B"]
        rows << ['General Error Message', "${progress.runError}"]
        rows << ['Overall Throughput (files)', "${DisplayUtil.simpleSize(xputFiles)}/s"]
        rows << ['Overall Throughput (bytes)', "${DisplayUtil.simpleSize(xputBytes)}B/s"]
        rows << ['Overall CPU Usage', "${cpuUsage.trunc(1)}%"]

        def entry = new HistoryEntry([configService: configService, jobId: jobId.toLong(), startTime: startTime])
        entry.syncResult = new SyncResult([config: sync, progress: progress])
        entry.write()
        configService.writeConfigObject(entry.reportKey, matrixToCsv(rows), 'text/csv')

        if (sync.options.dbFile || sync.options.dbTable) {
            def con = "${jobServer}/job/${jobId}/errors.csv".toURL().openConnection()
            configService.writeConfigObject(entry.errorsKey, con.inputStream, con.getHeaderField('Content-Type'))
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

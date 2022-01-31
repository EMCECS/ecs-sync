/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package sync.ui

import com.emc.ecs.sync.config.ConfigUtil
import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.rest.HostInfo
import com.emc.ecs.sync.rest.SyncProgress
import groovy.time.TimeCategory
import sync.ui.migration.AbstractMigration
import sync.ui.migration.MigrationHistoryEntry
import sync.ui.migration.MigrationResult
import sync.ui.storage.StorageEntry

import static grails.async.Promises.task
import static grails.async.Promises.waitAll

class HistoryService implements ConfigAccessor {
    def rest
    def jobServer
    def migrationService

    def archiveJob(jobId) {
        def (psync, pprogress, phost) = waitAll([
                task { rest.get("${jobServer}/job/${jobId}") { accept(SyncConfig.class) }.body },
                task { rest.get("${jobServer}/job/${jobId}/progress") { accept(SyncProgress.class) }.body },
                task { rest.get("${jobServer}/host") { accept(HostInfo.class) }.body }
        ])

        SyncConfig sync = psync as SyncConfig
        SyncProgress progress = pprogress as SyncProgress
        HostInfo host = phost as HostInfo

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

        // create and write history entry XML
        def entry = new SyncHistoryEntry([configService: configService, jobName: sync.jobName, startTime: startTime])
        entry.syncResult = new SyncResult([config: sync, progress: progress])
        entry.write()

        // write report CSV
        configService.writeConfigObject(entry.reportKey, matrixToCsv(rows), 'text/csv')

        // write error report CSV
        // TODO: make sure we have a configured database connection, or skip this
        if (sync.options.dbFile || sync.options.dbTable) {
            def con = "${jobServer}/job/${jobId}/errors.csv".toURL().openConnection()
            configService.writeConfigObject(entry.errorsKey, con.inputStream, con.getHeaderField('Content-Type'))
        }

        entry
    }

    def archiveMigration(guid) {
        AbstractMigration migration = migrationService.lookup(guid) as AbstractMigration

        def sourceEntry = new StorageEntry([xmlKey: migration.migrationConfig.sourceXmlKey])
        def targetEntry = new StorageEntry([xmlKey: migration.migrationConfig.targetXmlKey])
        def startTime = new Date(migration.startTime)

        def rows = [[]]
        rows << ['Description:', "${migration.migrationConfig.description}"]
        rows << ['Percent Complete:', "${migration.percentComplete()}%"]
        rows << ['Status', "${migration.state}"]
        rows << ['Source', "${sourceEntry.type.capitalize()} - ${sourceEntry.name}"]
        rows << ['Target', "${targetEntry.type.capitalize()} - ${targetEntry.name}"]
        rows << ['Started At', "${startTime.format('yyyy-MM-dd hh:mm:ssa')}"]
        rows << ['Stopped At', "${migration.stopTime ? new Date(migration.stopTime).format('yyyy-MM-dd hh:mm:ssa') : 'N/A'}"]
        rows << ['Duration', "${DisplayUtil.shortDur(TimeCategory.minus(new Date(migration.duration()), new Date(0)))}"]
        rows << ['Total Tasks', "${migration.taskList.size()}"]
        rows << ['Task Details:', '% Complete', 'Status', 'Message', 'Start Time', 'Stop Time', 'Task Info']
        migration.taskList.each { task ->
            def taskStart = task.startTime ? new Date(task.startTime).format('yyyy-MM-dd hh:mm:ssa') : 'N/A'
            def taskStop = task.stopTime ? new Date(task.stopTime).format('yyyy-MM-dd hh:mm:ssa') : 'N/A'
            rows << ["${task.name}", "${task.percentComplete}", "${task.taskStatus}", "${task.taskMessage}", taskStart, taskStop, "${task.description}"]
        }

        // write history entry XML
        def entry = new MigrationHistoryEntry([configService: configService, name: migration.migrationConfig.shortName, startTime: new Date(migration.startTime)])
        entry.migrationResult = new MigrationResult([config: migration.migrationConfig])
        entry.write()

        // write report CSV
        configService.writeConfigObject(entry.reportKey, matrixToCsv(rows), 'text/csv')

        entry
    }

    private static String matrixToCsv(rows) {
        def csv = ''
        rows.each { row ->
            row.eachWithIndex { col, i ->
                if (i > 0) csv += ','
                csv += '"' + col.replaceAll('"', '""') + '"'
            }
            csv += '\n'
        }
        return csv
    }
}

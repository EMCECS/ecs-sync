%{--
  - Copyright (c) 2015-2018 Dell Inc. or its subsidiaries. All Rights Reserved.
  -
  - Licensed under the Apache License, Version 2.0 (the "License");
  - you may not use this file except in compliance with the License.
  - You may obtain a copy of the License at
  -
  - http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing, software
  - distributed under the License is distributed on an "AS IS" BASIS,
  - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  - See the License for the specific language governing permissions and
  - limitations under the License.
  --}%
<%@ page import="com.emc.ecs.sync.config.ConfigUtil; com.emc.ecs.sync.rest.JobControlStatus; groovy.time.TimeCategory" contentType="text/html;charset=UTF-8" %>
<%@ page import="sync.ui.SyncUtil" %>
<%@ page import="sync.ui.DisplayUtil" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <meta http-equiv="refresh" content="5">
    <title>ECS Sync UI</title>
</head>

<body>

<h3>
    ${ConfigUtil.wrapperFor(sync.source.getClass()).label} <small style="font-weight: normal">${SyncUtil.getLocation(sync.source)}</small> -&gt;
    ${ConfigUtil.wrapperFor(sync.target.getClass()).label} <small style="font-weight: normal">${SyncUtil.getLocation(sync.target)}
    <g:if test="${sync.jobName}"><br><span class="sync-job-name">${sync.jobName}</span></g:if></small>
</h3>

    <div style="float: right; width: auto; margin-left: 40px; margin-top: -12px">
        <g:link action="resume" params="[jobId: jobId, fromAction: 'show']" class="btn btn-lg btn-success ${!(control.status in [JobControlStatus.Pausing,JobControlStatus.Paused]) ? 'disabled' : ''}">&#x25ba;</g:link>
        <g:link action="pause" params="[jobId: jobId, fromAction: 'show']" class="btn btn-lg btn-warning ${control.status != JobControlStatus.Running ? 'disabled' : ''}">| |</g:link>
        <g:link action="stop" params="[jobId: jobId, fromAction: 'show']" class="btn btn-lg btn-danger ${!(control.status in [JobControlStatus.Running,JobControlStatus.Pausing,JobControlStatus.Paused]) ? 'disabled' : ''}">&#x25fc;</g:link>
    </div>
    <div class="progress">
        <div class="progress-bar progress-bar-info progress-bar-striped" role="progressbar"
             aria-valuenow="${progress.estimatingTotals ? 'N/A' : progressPercent?.trunc(2)}" aria-valuemin="0" aria-valuemax="100" style="width:${progressPercent?.trunc(2)}%">
            ${progress.estimatingTotals ? 'N/A' : progressPercent?.trunc(2)}% Complete
        </div>
    </div>

    <h4 class="text-capitalize">${control.status}
        <g:if test="${control.status.finalState}"><div style="float: right">
        <g:if test="${SyncUtil.generatedTable(sync)}">
            <g:set var="confirmArchive" value="return confirm('Archive this job?\\nNote: this action will also delete the corresponding database')" />
        </g:if>
        <g:link controller="sync" action="restart" params="[jobId: jobId]" class="btn btn-sm btn-info"
                title="Re-run this sync again to retry any errors">&#x21bb;</g:link>
        <g:if test="${config}"><g:link controller="sync" action="archive" params="[jobId: jobId]" class="btn btn-sm btn-primary"
                title="Archive (the information here is available in the sync report)"
                onclick="${confirmArchive}">&#x2713;</g:link></g:if>
        <g:if test="${sync.options.dbTable}"><g:link controller="sync" action="allObjectReport" params="[jobId: jobId]" class="btn btn-sm btn-primary"
                title="Complete Object Report (note: this contains a record of every file or object transferred and can be quite large)"
                onclick="return confirm('Download Complete Object Report?\\nNote: this contains a record of every file or object transferred and can be quite large')">&#x2193;&#x1f4c4;</g:link></g:if><!-- &#x1f4d2; &#x1f5b9; &#x1f5ce; -->
        </div></g:if><g:elseif test="${control.threadCount}"><span style="float: right">Thread Count: <g:each in="${[8,16,24,32,40,48,64,80,100]}">
        <g:link action="setThreads" params="[jobId: jobId, threadCount: it, fromAction: 'show']" class="btn btn-primary ${control.threadCount == it ? 'disabled' : ''}">${it}</g:link>
    </g:each></span></g:elseif>
    </h4>

    <table class="table table-striped table-condensed" style="margin-top:30px;">
        <colgroup>
            <col width="200">
            <col width="*">
        </colgroup>
    <tr><th>Started:</th><td><g:if test="${progress.syncStartTime}">${new Date(progress.syncStartTime).format('yyyy-MM-dd hh:mm:ssa')}</g:if></td></tr>
    <tr><th>Run Time: </th><td>${DisplayUtil.shortDur(TimeCategory.minus(new Date(progress.runtimeMs), new Date(0)))}</td></tr>
    <g:if test="${control.status.finalState}">
      <tr><th>Stopped:</th><td>${new Date(progress.syncStopTime).format('yyyy-MM-dd hh:mm:ssa')}</td></tr>
    </g:if><g:else>
      <tr><th>Est. Time Remaining: </th><td>${!progress.estimatingTotals && msRemaining > 0 ? DisplayUtil.shortDur(TimeCategory.minus(new Date(msRemaining), new Date(0))) : 'N/A'}</td></tr>
    </g:else>
    <tr><th>Thread Count: </th><td>${control.threadCount}</td></tr>
    <tr><td colspan="2">${String.format('%,d', progress.objectsComplete + progress.objectsSkipped)} /
            ${progress.totalObjectsExpected >= 0 ? String.format('%,d', progress.totalObjectsExpected) : 'N/A'} objects processed
            <g:if test="${progress.estimatingTotals}"><span class="alert-info">(calculating)</span></g:if>
            <g:if test="${progress.objectsSkipped > 0}">(${String.format('%,d', progress.objectsSkipped)} skipped)</g:if></td></tr>
    <tr><td colspan="2">${DisplayUtil.simpleSize(progress.bytesComplete + progress.bytesSkipped)} /
            ${progress.totalBytesExpected >= 0 ? DisplayUtil.simpleSize(progress.totalBytesExpected) : 'N/A'} bytes processed
            <g:if test="${progress.estimatingTotals}"><span class="alert-info">(calculating)</span></g:if>
            <g:if test="${progress.bytesSkipped > 0}">(${DisplayUtil.simpleSize(progress.bytesSkipped)}B skipped)</g:if></td></tr>
    <tr><th>Current Source Bandwidth: </th><td>${DisplayUtil.simpleSize(progress.sourceReadRate)}B/s Read,
            ${DisplayUtil.simpleSize(progress.sourceWriteRate)}B/s Write</td></tr>
    <tr><th>Current Target Bandwidth: </th><td>${DisplayUtil.simpleSize(progress.targetReadRate)}B/s Read,
            ${DisplayUtil.simpleSize(progress.targetWriteRate)}B/s Write</td></tr>
    <tr><th>Current Throughput: </th><td>${progress.objectCompleteRate + progress.objectSkipRate}/s</td></tr>
    <tr><th>Current Error Rate: </th><td>${progress.objectErrorRate}/s</td></tr>
    <tr><th>Process CPU: </th><td>${progress.processCpuLoad ? (progress.processCpuLoad * 100d).trunc(1)+'%' : 'N/A'}</td></tr>
    <tr><th>Process Memory: </th><td>${DisplayUtil.simpleSize(memorySize)}B</td></tr>
    <tr><th>Active Tasks: </th><td>${progress.activeSyncTasks}</td></tr>
    <tr><th>Overall Throughput: </th><td>${((progress.objectsComplete + progress.objectsSkipped) / (progress.runtimeMs ?: 1L) * 1000).toLong()}/s</td></tr>
    <tr><th>Overall Bandwidth: </th><td>${DisplayUtil.simpleSize(((progress.bytesComplete) / (progress.runtimeMs ?: 1L)).toLong() * 1000)}B/s</td></tr>
    <tr><th>Overall CPU: </th><td>${overallCpu.trunc(1)}%</td></tr>
</table>

    <h2>Errors</h2>

    <table class="table table-striped table-condensed">
        <colgroup>
            <col width="300">
            <col width="*">
        </colgroup>
        <tr><th>General Error Message: </th><td>${progress.runError}</td></tr>
        <tr><th>Retry Queue Count: </th><td>${progress.objectsAwaitingRetry}</td></tr>
        <tr><td><g:link controller="sync" action="retries" params="[jobId: jobId]" target="_blank" class="btn btn-primary">Generate Retry Queue Report</g:link></td></tr>
        <tr><th>Error Count: </th><td>${progress.objectsFailed}</td></tr>
        <tr><td><g:link controller="sync" action="errors" params="[jobId: jobId]" target="_blank" class="btn btn-primary">Generate Error Report</g:link></td></tr>
    </table>

    <h2>Current Host Stats</h2>
    <table class="table table-striped table-condensed">
        <colgroup>
            <col width="300">
            <col width="*">
        </colgroup>
        <tr><th>CPU:</th><td>${hostStats.hostCpuLoad ? (hostStats.hostCpuLoad * 100d).trunc(1)+'%' : 'N/A'}</td></tr>
        <tr><th>Memory:</th><td>${DisplayUtil.simpleSize(hostStats?.hostMemoryUsed)}B /
                ${DisplayUtil.simpleSize(hostStats?.hostTotalMemory)}B</td></tr>
        <tr><th>ECS Sync Version:</th><td>${hostStats?.ecsSyncVersion}</td></tr>
        <g:set var="logOnclickMap" value="${[debug:'return confirm("Are you sure you want to enable debug?\\nWARNING: Debug mode produces tremendous output. Be sure you have enough disk space to store the log file")']}"/>
        <tr><th>Log Level:</th><td><g:each in="${['silent', 'quiet', 'verbose', 'debug']}">
            <g:link action="setLogLevel" params="[logLevel: it, jobId: jobId, fromAction: 'show']" onclick="${logOnclickMap[it]}" class="btn btn-primary ${hostStats?.logLevel as String == it ? 'disabled' : ''}">${it}</g:link>
        </g:each></td></tr>
    </table>

</body>
</html>
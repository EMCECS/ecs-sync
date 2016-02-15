<%@ page import="groovy.time.TimeCategory" contentType="text/html;charset=UTF-8" %>
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
    ${sync.Source.@class.text().split('[.]').last().replaceAll(/Source$/, '')} <small style="font-weight: normal">${SyncUtil.getSourcePath(sync)}</small> -&gt;
    ${sync.Target.@class.text().split('[.]').last().replaceAll(/Target$/, '')} <small style="font-weight: normal">${SyncUtil.getTargetPath(sync)}</small>
</h3>

    <div style="float: right; width: auto; margin-left: 40px; margin-top: -12px">
        <g:link action="resume" params="[jobId: jobId, fromAction: 'show']" class="btn btn-lg btn-success ${control.Status != 'Paused' ? 'disabled' : ''}">&#x25ba;</g:link>
        <g:link action="pause" params="[jobId: jobId, fromAction: 'show']" class="btn btn-lg btn-warning ${control.Status != 'Running' ? 'disabled' : ''}">| |</g:link>
        <g:link action="stop" params="[jobId: jobId, fromAction: 'show']" class="btn btn-lg btn-danger ${control.Status in ['Initialized','Stopped','Stopping','Complete'] ? 'disabled' : ''}">&#x25fc;</g:link>
    </div>
    <div class="progress">
        <div class="progress-bar progress-bar-info progress-bar-striped" role="progressbar"
             aria-valuenow="${progressPercent?.trunc(2)}" aria-valuemin="0" aria-valuemax="100" style="width:${progressPercent?.trunc(2)}%">
            ${progressPercent?.trunc(2)}% Complete
        </div>
    </div>

    <h4 class="text-capitalize">${control.Status}
        <g:if test="${control.Status in ['Stopped','Complete']}"><div style="float: right">
        <g:link controller="sync" action="restart" params="[jobId: jobId]" class="btn btn-sm btn-info"
                title="Re-run this sync again to retry any errors">&#x21bb;</g:link>
        <g:link controller="sync" action="syncReport" params="[jobId: jobId]" class="btn btn-sm btn-primary"
                title="Archive (the information here is available in the sync report)">&#x2713;</g:link>
        </div></g:if><g:elseif test="${control.SyncThreadCount.text()}"><span style="float: right">Thread Count: <g:each in="${[16,24,32,40,48,64,80]}">
        <g:link action="setThreads" params="[jobId: jobId, threadCount: it, fromAction: 'show']" class="btn btn-primary ${control.SyncThreadCount.toLong() == it ? 'disabled' : ''}">${it}</g:link>
    </g:each></span></g:elseif>
    </h4>

    <table class="table table-striped table-condensed" style="margin-top:30px;">
        <colgroup>
            <col width="200">
            <col width="*">
        </colgroup>
    <tr><th>Started:</th><td>${new Date(progress.SyncStartTime.toLong()).format('yyyy-MM-dd hh:mma')}</td></tr>
    <tr><th>Run Time: </th><td>${DisplayUtil.shortDur(TimeCategory.minus(new Date(progress.RuntimeMs.toLong()), new Date(0)))}</td></tr>
    <tr><th>Est. Time Remaining: </th><td>${msRemaining > 0 ? DisplayUtil.shortDur(TimeCategory.minus(new Date(msRemaining), new Date(0))) : 'N/A'}</td></tr>
    <tr><td colspan="2">${DisplayUtil.simpleSize(progress.BytesComplete.toLong())} /
            ${progress.TotalBytesExpected.toLong() >= 0 ? DisplayUtil.simpleSize(progress.TotalBytesExpected.toLong()) : 'N/A'} bytes transferred
            <g:if test="${progress.EstimatingTotals.toBoolean()}"><span class="alert-info">(calculating)</span></g:if></td></tr>
    <tr><td colspan="2">${String.format('%,d', progress.ObjectsComplete.toLong())} /
            ${progress.TotalObjectsExpected.toLong() >= 0 ? String.format('%,d', progress.TotalObjectsExpected.toLong()) : 'N/A'} files transferred
            <g:if test="${progress.EstimatingTotals.toBoolean()}"><span class="alert-info">(calculating)</span></g:if></td></tr>
    <tr><th>Current Bandwidth: </th><td>${DisplayUtil.simpleSize(progress.SourceReadRate.toLong())}B/s Read,
            ${DisplayUtil.simpleSize(progress.TargetWriteRate.toLong())}B/s Write</td></tr>
    <tr><th>Current Throughput: </th><td>${progress.ObjectCompleteRate}/s</td></tr>
    <tr><th>Current Error Rate: </th><td>${progress.ObjectErrorRate}/s</td></tr>
    <tr><th>Process CPU: </th><td>${progress.ProcessCpuLoad.text() ? (progress.ProcessCpuLoad.toDouble() * 100).trunc(1)+'%' : 'N/A'}</td></tr>
    <tr><th>Process Memory: </th><td>${DisplayUtil.simpleSize(memorySize)}B</td></tr>
    <tr><th>Active Tasks: </th><td>${progress.ActiveSyncTasks}</td></tr>
    <tr><th>Overall Bandwidth: </th><td>${DisplayUtil.simpleSize((progress.BytesComplete.toLong() / (progress.RuntimeMs.toLong() ?: 1L)).toLong() * 1000)}B/s</td></tr>
    <tr><th>Overall Throughput: </th><td>${(progress.ObjectsComplete.toLong() / (progress.RuntimeMs.toLong() ?: 1L) * 1000).toLong()}/s</td></tr>
    <tr><th>Overall CPU: </th><td>${overallCpu.trunc(1)}%</td></tr>
</table>

    <h2>Errors</h2>

    <table class="table table-striped table-condensed table-nowrap">
        <colgroup>
            <col width="300">
            <col width="*">
        </colgroup>
        <tr><th>General Error Message: </th><td>${progress.RunError}</td></tr>
        <tr><th>Error Count: </th><td>${progress.ObjectsFailed}</td></tr>
        <tr><td><g:link controller="sync" action="errors" params="[jobId: jobId]" target="_blank" class="btn btn-primary">Generate Error Report</g:link></td></tr>
    </table>

    <h2>Current Host Stats</h2>
    <table class="table table-striped table-condensed">
        <colgroup>
            <col width="300">
            <col width="*">
        </colgroup>
        <tr><th>CPU:</th><td>${hostStats?.HostCpuLoad?.text() ? (hostStats.HostCpuLoad.toDouble() * 100).trunc(1)+'%' : 'N/A'}</td></tr>
        <tr><th>Memory:</th><td>${DisplayUtil.simpleSize(hostStats?.HostMemoryUsed?.toLong())}B /
                ${DisplayUtil.simpleSize(hostStats?.HostTotalMemory?.toLong())}B</td></tr>
        <tr><th>ECS Sync Version:</th><td>${hostStats?.EcsSyncVersion}</td></tr>
    </table>

</body>
</html>
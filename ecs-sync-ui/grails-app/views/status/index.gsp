<%@ page import="groovy.time.TimeCategory" contentType="text/html;charset=UTF-8" %>
<%@ page import="sync.ui.SyncUtil" %>
<%@ page import="sync.ui.DisplayUtil" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <meta http-equiv="refresh" content="20">
    <title>ECS Sync UI</title>
</head>

<body>

<g:if test="${jobs.Job.size() > 0}">
    <h2>Active Operations</h2>
</g:if>

<g:each in="${jobs.Job}">
    <g:set var="jobId" value="${it.JobId.toLong()}" />
    <g:set var="status" value="${it.Status}" />
    <g:set var="syncConfig" value="${it.Config}" />
    <g:set var="progress" value="${it.Progress}" />
    <g:set var="progressPercent" value="${progressPercents[jobId]}" />
    <g:set var="msRemaining" value="${msRemainings[jobId]}" />
    <g:set var="overallObjectRate" value="${progress.ObjectsComplete.toLong() * 1000 / (progress.RuntimeMs.toLong() ?: 1L)}" />
    <g:set var="overallByteRate" value="${progress.BytesComplete.toLong() * 1000 / (progress.RuntimeMs.toLong() ?: 1L)}" />

    <hr>

    <h4><g:link action="show" id="${jobId}">
        ${syncConfig.Source.@class.text().split('[.]').last().replaceAll(/Source$/, '')} <small style="font-weight: normal">${SyncUtil.getSourcePath(syncConfig)}</small> -&gt;
        ${syncConfig.Target.@class.text().split('[.]').last().replaceAll(/Target$/, '')} <small style="font-weight: normal">${SyncUtil.getTargetPath(syncConfig)}</small>
    </g:link></h4>

    <div style="float: right; width: auto; margin-left: 20px; margin-top: -5px">
        <g:link action="resume" params="[jobId: jobId]" class="btn btn-sm btn-success ${status != 'Paused' ? 'disabled' : ''}">&#x25ba;</g:link>
        <g:link action="pause" params="[jobId: jobId]" class="btn btn-sm btn-warning ${status != 'Running' ? 'disabled' : ''}">| |</g:link>
        <g:link action="stop" params="[jobId: jobId]" class="btn btn-sm btn-danger ${status in ['Initialized','Stopped','Stopping','Complete'] ? 'disabled' : ''}">&#x25fc;</g:link>
      <g:if test="${status in ['Stopped','Complete']}">
        <g:link controller="sync" action="syncReport" params="[jobId: jobId]" class="btn btn-sm btn-primary"
                title="Archive (the information here is available in the sync report)">&#x2713</g:link>
      </g:if>
    </div>
    <div class="progress"><g:link action="show" id="${jobId}">
        <div class="progress-bar progress-bar-info progress-bar-striped" role="progressbar"
             aria-valuenow="${progressPercent?.trunc(2)}" aria-valuemin="0" aria-valuemax="100" style="width:${progressPercent?.trunc(2)}%">
            ${progressPercent?.trunc(2)}% Complete
        </div>
    </g:link></div>

    <table style="width: 100%; border-top: none; margin-top: -5px">
        <tr>
            <td>ETA: ${msRemaining > 0 ? DisplayUtil.shortDur(TimeCategory.minus(new Date(msRemaining), new Date(0))) : 'N/A'}</td>
            <td>${String.format('%,d', progress.ObjectsComplete.toLong())} /
                ${progress.TotalObjectsExpected.toLong() >= 0 ? String.format('%,d', progress.TotalObjectsExpected.toLong()) : 'N/A'}
                files @ ${status == 'Complete' ? overallObjectRate.toLong() : progress.ObjectCompleteRate.toLong()}/s</td>
        </tr><tr>
            <td><g:link controller="sync" action="errors" params="[jobId: jobId]" target="_blank">
                ${progress.ObjectsFailed} errors</g:link></td>
            <td>${DisplayUtil.simpleSize(progress.BytesComplete.toLong())}B /
                ${progress.TotalBytesExpected.toLong() >= 0 ? DisplayUtil.simpleSize(progress.TotalBytesExpected.toLong()) : 'N/A'}B
                @ ${DisplayUtil.simpleSize(status == 'Complete' ? overallByteRate.toLong() : progress.TargetWriteRate.toLong())}B/s</td>
        </tr>
    </table>

</g:each>
<g:if test="${jobs.Job.size() == 0}">
    <h2>Idle (no active operations)</h2>

    <table class="table table-striped table-condensed">
        <colgroup>
            <col width="200">
            <col width="*">
        </colgroup>
        <tr><th>Last Sync Run: </th><td>${lastResult ? lastResult.startTime?.format("yyyy-MM-dd hh:mma") : 'N/A'}
            <g:if test="${lastResult?.syncResult?.config?.name}">(schedule: ${lastResult.syncResult.config.name})</g:if></td></tr>
    <g:if test="${lastResult}">
        <tr><th>Summary Report: </th><td><a href="${lastResult.reportUrl}">${lastResult.reportFileName}</a></td></tr>
      <g:if test="${lastResult.errorsExists}">
        <tr><th>Errors Report: </th><td><a href="${lastResult.errorsUrl}">${lastResult.errorsFileName}</a></td></tr>
      </g:if>
    </g:if>
    </table>
</g:if>

    <hr>
    <p>
        <g:link controller="sync" action="create" class="btn btn-primary">New Sync</g:link>
    </p>
    <hr>

    <h2>Current Host Stats</h2>
    <table class="table table-striped table-condensed">
        <colgroup>
            <col width="200">
            <col width="*">
        </colgroup>
        <tr><th>CPU:</th><td>${hostStats?.HostCpuLoad?.text() ? (hostStats.HostCpuLoad.toDouble() * 100).trunc(1)+'%' : 'N/A'}</td></tr>
        <tr><th>Memory:</th><td>${DisplayUtil.simpleSize(hostStats?.HostMemoryUsed?.toLong())}B /
                ${DisplayUtil.simpleSize(hostStats?.HostTotalMemory?.toLong())}B</td></tr>
        <tr><th>ECS Sync Version:</th><td>${hostStats?.EcsSyncVersion}</td></tr>
    </table>

</body>
</html>
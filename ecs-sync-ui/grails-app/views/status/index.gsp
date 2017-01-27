<%@ page import="com.emc.ecs.sync.config.ConfigUtil; com.emc.ecs.sync.rest.JobControlStatus; groovy.time.TimeCategory" contentType="text/html;charset=UTF-8" %>
<%@ page import="sync.ui.SyncUtil" %>
<%@ page import="sync.ui.DisplayUtil" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <meta http-equiv="refresh" content="20">
    <title>ECS Sync UI</title>
</head>

<body>

<g:if test="${jobs.jobs.size() > 0}">
    <h2>Active Operations</h2>
</g:if>

<g:each in="${jobs.jobs}">
    <g:set var="jobId" value="${it.jobId}" />
    <g:set var="status" value="${it.status}" />
    <g:set var="syncConfig" value="${it.config}" />
    <g:set var="progress" value="${it.progress}" />
    <g:set var="progressPercent" value="${progressPercents[jobId]}" />
    <g:set var="msRemaining" value="${msRemainings[jobId]}" />
    <g:set var="overallObjectRate" value="${(progress.objectsComplete + progress.objectsSkipped) * 1000 / (progress.runtimeMs ?: 1L)}" />
    <g:set var="overallByteRate" value="${(progress.bytesComplete + progress.bytesSkipped) * 1000 / (progress.runtimeMs ?: 1L)}" />

    <hr>

    <h4><g:link action="show" id="${jobId}">
        ${ConfigUtil.wrapperFor(syncConfig.source.getClass()).label} <small style="font-weight: normal">${SyncUtil.getLocation(syncConfig.source)}</small> -&gt;
        ${ConfigUtil.wrapperFor(syncConfig.target.getClass()).label} <small style="font-weight: normal">${SyncUtil.getLocation(syncConfig.target)}</small>
    </g:link></h4>

    <div style="float: right; width: auto; margin-left: 20px; margin-top: -5px">
        <g:link action="resume" params="[jobId: jobId]" class="btn btn-sm btn-success ${status != JobControlStatus.Paused ? 'disabled' : ''}">&#x25ba;</g:link>
        <g:link action="pause" params="[jobId: jobId]" class="btn btn-sm btn-warning ${status != JobControlStatus.Running ? 'disabled' : ''}">| |</g:link>
        <g:link action="stop" params="[jobId: jobId]" class="btn btn-sm btn-danger ${!(status in [JobControlStatus.Running,JobControlStatus.Paused]) ? 'disabled' : ''}">&#x25fc;</g:link>
      <g:if test="${status.finalState}">
        <g:if test="${SyncUtil.generatedTable(syncConfig)}">
          <g:set var="confirmArchive" value="return confirm('Archive this job?\\nNote: this action will also delete the corresponding database')" />
        </g:if>
        <g:link controller="sync" action="archive" params="[jobId: jobId]" class="btn btn-sm btn-primary"
                title="Archive (the information here is available in the sync report)"
                onclick="${confirmArchive}">&#x2713</g:link>
      </g:if>
    </div>
    <div class="progress"><g:link action="show" id="${jobId}">
        <div class="progress-bar progress-bar-info progress-bar-striped" role="progressbar"
             aria-valuenow="${progress.estimatingTotals ? 'N/A' : progressPercent?.trunc(2)}" aria-valuemin="0" aria-valuemax="100" style="width:${progressPercent?.trunc(2)}%">
            ${progress.estimatingTotals ? 'N/A' : progressPercent?.trunc(2)}% Complete
        </div>
    </g:link></div>

    <table style="width: 100%; border-top: none; margin-top: -5px">
        <tr>
            <td>ETA: ${!progress.estimatingTotals && msRemaining > 0 ? DisplayUtil.shortDur(TimeCategory.minus(new Date(msRemaining), new Date(0))) : 'N/A'}</td>
            <td>${String.format('%,d', progress.objectsComplete + progress.objectsSkipped)} /
                ${progress.totalObjectsExpected >= 0 ? String.format('%,d', progress.totalObjectsExpected) : 'N/A'}
                files @ ${status == JobControlStatus.Complete ? overallObjectRate.toLong() : progress.objectCompleteRate + progress.objectSkipRate}/s</td>
        </tr><tr>
            <td><g:link controller="sync" action="errors" params="[jobId: jobId]" target="_blank">
                ${progress.objectsFailed} errors</g:link></td>
            <td>${DisplayUtil.simpleSize(progress.bytesComplete + progress.bytesSkipped)}B /
                ${progress.totalBytesExpected >= 0 ? DisplayUtil.simpleSize(progress.totalBytesExpected) : 'N/A'}B
                @ ${DisplayUtil.simpleSize(status == JobControlStatus.Complete ? overallByteRate.toLong() : progress.sourceReadRate)}B/s</td>
        </tr>
    </table>

</g:each>
<g:if test="${jobs.jobs.size() == 0}">
    <h2>Idle (no active operations)</h2>

    <table class="table table-striped table-condensed">
        <colgroup>
            <col width="200">
            <col width="*">
        </colgroup>
        <tr><th>Last Sync Run: </th><td>${lastArchive ? lastArchive.startTime?.format("yyyy-MM-dd hh:mma") : 'N/A'}
            <g:if test="${lastArchive?.syncResult?.config?.properties?.scheduleName}">(schedule: ${lastArchive.syncResult.config.properties?.scheduleName})</g:if></td></tr>
    <g:if test="${lastArchive}">
        <tr><th>Summary Report: </th><td><a href="${lastArchive.reportUri}">${lastArchive.reportFileName}</a></td></tr>
      <g:if test="${lastArchive.errorsExists}">
        <tr><th>Errors Report: </th><td><a href="${lastArchive.errorsUri}">${lastArchive.errorsFileName}</a></td></tr>
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
        <tr><th>CPU:</th><td>${hostStats?.hostCpuLoad?.trunc(1)+'%'}</td></tr>
        <tr><th>Memory:</th><td>${DisplayUtil.simpleSize(hostStats?.hostMemoryUsed)}B /
                ${DisplayUtil.simpleSize(hostStats?.hostTotalMemory)}B</td></tr>
        <tr><th>ECS Sync Version:</th><td>${hostStats?.ecsSyncVersion}</td></tr>
        <g:set var="logOnclickMap" value="${[debug:'return confirm("Are you sure you want to enable debug?\\nWARNING: Debug mode produces tremendous output. Be sure you have enough disk space to store the log file")']}"/>
        <tr><th>Log Level:</th><td><g:each in="${['silent', 'quiet', 'verbose', 'debug']}">
            <g:link action="setLogLevel" params="[logLevel: it]" onclick="${logOnclickMap[it]}" class="btn btn-primary ${hostStats?.logLevel as String == it ? 'disabled' : ''}">${it}</g:link>
        </g:each></td></tr>
    </table>

</body>
</html>
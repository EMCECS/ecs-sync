<%@ page import="com.emc.ecs.sync.config.ConfigUtil; sync.ui.SyncUtil" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>

<h2>Migration History</h2>
<table class="table table-striped">
    <g:if test="${migrationHistoryEntries}">
        <g:each in="${migrationHistoryEntries}" var="entry">
            <g:set var="migrationStart" value="${entry.startTime.format("yyyy-MM-dd'<br>'hh:mm:ssa")}"/>
            <tr><td>${raw(migrationStart)}</td>
                <td>${entry.migrationResult.config.description}</td>
                <td><a href="${entry.reportUri}" download="${entry.reportFileName}">Report</a></td>
                <td><g:link action="deleteMigrationEntry" params="[entryId: entry.id]" onclick="return confirm('Delete ${migrationStart}\\n(${entry.migrationResult.config.description})?')" class="btn btn-sm btn-danger">x</g:link></td></tr>
        </g:each>
    </g:if>
    <g:else>
        <tr><td>No archived migrations</td></tr>
    </g:else>
</table>

<h2>Job History</h2>
<table class="table table-striped">
    <g:if test="${syncHistoryEntries}">
        <g:each in="${syncHistoryEntries}" var="entry">
            <g:set var="jobStart" value="${new Date(entry.syncResult.progress.syncStartTime).format("yyyy-MM-dd'<br>'hh:mm:ssa")}"/>
            <tr><td>${entry.syncResult.config.properties.scheduleName}</td>
                <td>${raw(jobStart)}</td>
                <td><g:if test="${entry.syncResult.config.jobName}"><span class="sync-job-name">${entry.syncResult.config.jobName}</span><br></g:if>
                    <strong>${ConfigUtil.wrapperFor(entry.syncResult.config.source.getClass()).label}</strong>
                    <small>(${SyncUtil.getLocation(entry.syncResult.config.source)})</small> -&gt;<br>
                    <strong>${ConfigUtil.wrapperFor(entry.syncResult.config.target.getClass()).label}</strong>
                    <small>(${SyncUtil.getLocation(entry.syncResult.config.target)})</small></td>
                <td><a href="${entry.reportUri}" download="${entry.reportFileName}">Report</a></td>
                <td><g:link action="getSyncXml" params="[entryId: entry.id]">XML</g:link></td>
                <td><g:if test="${entry.errorsExists}"><a href="${entry.errorsUri}" download="${entry.errorsFileName}">Errors</a></g:if></td>
                <td><g:link controller="sync" action="copyArchived" params="[entryId: entry.id]" class="btn btn-sm btn-info"
                            title="Create a new sync job as a duplicate of this one"><span class="glyphicon glyphicon-duplicate"></span></g:link>
                    <g:link action="deleteSyncEntry" params="[entryId: entry.id]" onclick="return confirm('Delete ${jobStart}\\n(${SyncUtil.getSyncDesc(entry.syncResult.config)})?')" class="btn btn-sm btn-danger">x</g:link></td></tr>
        </g:each>
    </g:if>
    <g:else>
        <tr><td>No archived jobs</td></tr>
    </g:else>
</table>

</body>
</html>
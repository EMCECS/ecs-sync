<%@ page import="com.emc.ecs.sync.config.ConfigUtil; sync.ui.SyncUtil" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>

<h2>Archive History</h2>
<table class="table table-striped">
    <g:if test="${archives}">
        <g:each in="${archives}" var="archive">
            <g:set var="archiveStart" value="${new Date(archive.syncResult.progress.syncStartTime).format("yyyy-MM-dd'<br>'hh:mm:ssa")}"/>
            <tr><td>${archive.syncResult.config.properties.scheduleName}</td>
                <td>${raw(archiveStart)}</td>
                <td><strong>${ConfigUtil.wrapperFor(archive.syncResult.config.source.getClass()).label}</strong>
                    <small>(${SyncUtil.getLocation(archive.syncResult.config.source)})</small> -&gt;<br>
                    <strong>${ConfigUtil.wrapperFor(archive.syncResult.config.target.getClass()).label}</strong>
                    <small>(${SyncUtil.getLocation(archive.syncResult.config.target)})</small></td>
                <td><a href="${archive.reportUri}">Report</a></td>
                <td><a href="${archive.xmlUri}">XML</a></td>
                <td><g:if test="${archive.errorsExists}"><a href="${archive.errorsUri}">Errors</a></g:if></td>
                <td><g:link controller="sync" action="copyArchived" params="[archiveId: archive.id]" class="btn btn-sm btn-info"
                            title="Create a new sync job as a duplicate of this one"><span class="glyphicon glyphicon-duplicate"></span></g:link>
                    <g:link action="delete" params="[archiveId: archive.id]" onclick="return confirm('Delete ${archiveStart}\\n(${SyncUtil.getSyncDesc(archive.syncResult.config)})?')" class="btn btn-sm btn-danger">x</g:link></td></tr>
        </g:each>
    </g:if>
    <g:else>
        <tr><td>No archived jobs</td></tr>
    </g:else>
</table>

</body>
</html>
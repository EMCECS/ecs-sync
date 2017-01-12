<%@ page import="com.emc.ecs.sync.config.ConfigUtil; sync.ui.SyncUtil" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>

<g:if test="${flash.message}">
    <div class="alert alert-info" role="status">${flash.message}</div>
</g:if>

<g:if test="${scheduleEntries.size() > 0}">
    <h2>Scheduled Syncs</h2>

    <table class="table table-striped">
        <g:each in="${scheduleEntries}">
            <tr>
                <td><g:link action="edit" params="[name: it.name]">${it.name}</g:link></td>
                <td><strong>${ConfigUtil.wrapperFor(it.scheduledSync.config.source.getClass()).label}</strong> <small>${SyncUtil.getLocation(it.scheduledSync.config.source)}</small> -&gt;
                    <strong>${ConfigUtil.wrapperFor(it.scheduledSync.config.target.getClass()).label}</strong> <small>${SyncUtil.getLocation(it.scheduledSync.config.target)}</small></td>
                <td><g:link action="delete" params="[name: it.name]" onclick="return confirm('Delete ${it.name}?')" class="btn btn-sm btn-danger">X</g:link></td>
            </tr>
        </g:each>
    </table>
</g:if><g:else>
    <h2>No Scheduled Syncs</h2>
</g:else>

<hr>

<p>
    <g:link action="create" class="btn btn-primary">Schedule a Sync</g:link>
</p>

</body>
</html>
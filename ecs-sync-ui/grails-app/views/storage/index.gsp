<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>

<h2>Storage Clusters</h2>
<table class="table table-striped">
    <g:if test="${storageEntries}">
        <tr><th>Name</th><th>Type</th><th>&nbsp;</th><th>&nbsp;</th></tr>
        <g:each in="${storageEntries}" var="entry">
            <tr><td><g:link action="edit" params="[storageKey: entry.xmlKey]">${entry.name}</g:link></td>
                <td><strong>${entry.type.capitalize()}</strong></td>
                <td><g:link controller="${entry.type}" action="show" params="[storageKey: entry.xmlKey]">Browse</g:link></td>
                <td><g:link action="delete" params="[storageKey: entry.xmlKey]" onclick="return confirm('Delete configuration for ${entry.name}?')" class="btn btn-sm btn-danger">x</g:link></td></tr>
        </g:each>
    </g:if>
    <g:else>
        <tr><td>No configured storage clusters</td></tr>
    </g:else>
</table>

<hr>
<p>
    <g:link action="create" params="['type': 'atmos']" class="btn btn-primary">Add Atmos Cluster</g:link>
    <g:link action="create" params="['type': 'ecs']" class="btn btn-primary">Add ECS Cluster</g:link>
</p>

</body>
</html>
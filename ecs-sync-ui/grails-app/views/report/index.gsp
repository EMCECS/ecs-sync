<%@ page import="sync.ui.DisplayUtil" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>

<h2>Completion Reports</h2>
<table class="table table-striped">
    <g:if test="${results}">
        <g:each in="${results}" var="result">
            <tr><td><a href="${result.reportUrl}">${result.reportFileName}</a></td>
                <td><g:if test="${result.errorsExists}"><a href="${result.errorsUrl}">Errors</a></g:if></td>
                <td><g:link action="delete" params="[resultId: result.id]" onclick="return confirm('Delete ${result.id}?')" class="btn btn-sm btn-danger">x</g:link></td></tr>
        </g:each>
    </g:if>
    <g:else>
        <tr><td>No reports available</td></tr>
    </g:else>
</table>

</body>
</html>
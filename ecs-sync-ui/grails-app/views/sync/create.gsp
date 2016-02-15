<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>

<h2>New Sync</h2>

<g:form action="start" method="post" accept-charset="UTF-8">
    <g:render template="syncConfig" model="[uiConfig: uiConfig, syncConfig: syncConfig, prefix: 'syncConfig']" />
    <g:submitButton name="Start" value="Start" class="btn btn-primary" />
</g:form>

</body>
</html>
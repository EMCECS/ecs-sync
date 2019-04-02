<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>

<h3>ECS Cluster ${storageEntry.name}</h3>

<h4>Namespaces</h4>
<table class="table table-striped table-condensed">
    <tr><th>Name</th><th>URN</th></tr>
    <g:each in="${namespaces}" var="namespace">
        <tr><td>${namespace.name}</td><td>${namespace.id}</td></tr>
    </g:each>
</table>

</body>
</html>
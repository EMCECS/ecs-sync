<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>

<h3>Atmos Subtenant ${subtenant.name}</h3>

<table class="table table-striped table-condensed" style="margin-top:30px;">
    <colgroup>
        <col width="200">
        <col width="*">
    </colgroup>
    <tr><th>Subtenant ID</th><td>${subtenant.id}</td></tr>
    <tr><th>Status</th><td>${subtenant.status}</td></tr>
    <tr><th>Capacity</th><td>${subtenant.capacity}</td></tr>
    <tr><th>Auth Source</th><td>${subtenant.authenticationSource}</td></tr>
    <tr><th>SEC Compliant</th><td>${subtenant.secCompliant}</td></tr>
</table>

<h4>Users</h4>
<table class="table table-striped table-condensed">
    <tr><th>UID</th><th>Email</th><th>Status</th></tr>
    <g:each in="${subtenant.objectUsers}" var="user">
        <tr><td>${user.uid}</td><td>${user.email}</td><td>${user.status}</td></tr>
    </g:each>
</table>

</body>
</html>
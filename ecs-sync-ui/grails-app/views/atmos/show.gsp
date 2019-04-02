<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>

<h3>Atmos Tenant ${tenant.name}</h3>

<table class="table table-striped table-condensed" style="margin-top:30px;">
    <colgroup>
        <col width="200">
        <col width="*">
    </colgroup>
    <tr><th>Tenant ID</th><td>${tenant.id}</td></tr>
    <tr><th>Status</th><td>${tenant.status}</td></tr>
    <tr><th>Capacity</th><td>${tenant.capacity}</td></tr>
    <tr><th>Auth Source</th><td>${tenant.authenticationSource}</td></tr>
    <tr><th colspan="2">Nodes</th></tr><g:each in="${tenant.accessNodeList}">
    <tr><th></th><td>${it.name} (${it.publicIp})</td></tr></g:each>
</table>

<h4>Subtenants</h4>
<table class="table table-striped table-condensed">
    <tr><th>Name</th><th>Id</th></tr>
  <g:each in="${tenant.subtenantList}" var="subtenant">
    <tr><td><g:link action="subtenant" params="[storageKey: params.storageKey, subtenantName: subtenant.name]">${subtenant.name}</g:link></td><td>${subtenant.id}</td></tr>
  </g:each>
</table>

</body>
</html>
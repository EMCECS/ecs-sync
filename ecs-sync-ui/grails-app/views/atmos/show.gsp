%{--
  - Copyright (c) 2018 Dell Inc. or its subsidiaries. All Rights Reserved.
  -
  - Licensed under the Apache License, Version 2.0 (the "License");
  - you may not use this file except in compliance with the License.
  - You may obtain a copy of the License at
  -
  - http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing, software
  - distributed under the License is distributed on an "AS IS" BASIS,
  - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  - See the License for the specific language governing permissions and
  - limitations under the License.
  --}%
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
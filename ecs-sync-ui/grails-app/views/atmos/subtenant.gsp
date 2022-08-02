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
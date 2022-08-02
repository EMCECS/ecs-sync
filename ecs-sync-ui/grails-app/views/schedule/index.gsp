%{--
  - Copyright (c) 2016-2017 Dell Inc. or its subsidiaries. All Rights Reserved.
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
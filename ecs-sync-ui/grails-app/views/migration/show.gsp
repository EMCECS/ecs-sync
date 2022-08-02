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
<%@ page import="sync.ui.SyncUtil" %>
<%@ page import="sync.ui.DisplayUtil" %>
<%@ page import="groovy.time.TimeCategory" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>

<h3><strong>${migrationEntry.type.capitalize()}</strong>: ${migrationEntry.migrationConfig.description}</h3>

    <div style="float: right; width: auto; margin-left: 40px; margin-top: -12px">
        <g:link action="resume" params="[migrationKey: migrationEntry.xmlKey, fromAction: 'show']"
                class="btn btn-lg btn-success ${!activeMigration?.isPaused() ? 'disabled' : ''}"
                onclick="return confirm('Are you sure you want to resume this migration: ${migrationEntry.migrationConfig.description}')">&#x25ba;</g:link>
        <g:link action="pause" params="[migrationKey: migrationEntry.xmlKey, fromAction: 'show']"
                class="btn btn-lg btn-warning ${!activeMigration?.isRunning() ? 'disabled' : ''}">| |</g:link>
        <g:link action="stop" params="[migrationKey: migrationEntry.xmlKey, fromAction: 'show']"
                class="btn btn-lg btn-danger ${!(activeMigration?.isRunning() || activeMigration?.isPaused()) ? 'disabled' : ''}"
                onclick="return confirm('Are you sure you want to stop this migration: ${migrationEntry.migrationConfig.description}')">&#x25fc;</g:link>
    </div>
    <div class="progress">
        <div class="progress-bar progress-bar-info progress-bar-striped" role="progressbar"
             aria-valuenow="${activeMigration?.percentComplete()}" aria-valuemin="0" aria-valuemax="100" style="width:${activeMigration?.percentComplete()}%">
            ${activeMigration?.percentComplete()}% Complete
        </div>
    </div>

    <h4 class="text-capitalize"><g:if test="${activeMigration}">${activeMigration?.state}
        <div style="float: right">
        <g:if test="${activeMigration?.isStopped()}"><g:link action="archive" params="[migrationKey: migrationEntry.xmlKey]" class="btn btn-sm btn-warning"
              title="Archive (the information here is available in the migration report)"
              onclick="return confirm('Are you sure you want to archive this migration: ${migrationEntry.migrationConfig.description}')">&#x2713;</g:link></g:if>
        </div>
    </g:if><g:else>WARNING: this migration is not active
        <div style="float: right">
        <g:link action="restart" params="[migrationKey: migrationEntry.xmlKey, fromAction: 'show']" class="btn btn-sm btn-warning"
                title="Restart this migration"
                onclick="return confirm('Are you sure you want to restart this migration?')">&#x21bb;</g:link>
        </div>
    </g:else></h4>

    <table class="table table-striped table-condensed" style="margin-top:30px;">
        <colgroup>
            <col width="200">
            <col width="*">
        </colgroup>
        <tr><th>Started:</th><td><g:if test="${activeMigration?.startTime}">${new Date(activeMigration.startTime).format('yyyy-MM-dd hh:mm:ssa')}</g:if></td></tr>
        <tr><th>Run Time: </th><td>${DisplayUtil.shortDur(TimeCategory.minus(new Date(activeMigration?.duration() ?: 0), new Date(0)))}</td></tr>
        <g:if test="${activeMigration?.stopTime}">
          <tr><th>Stopped:</th><td>${new Date(activeMigration.stopTime).format('yyyy-MM-dd hh:mm:ssa')}</td></tr>
        </g:if>
    </table>

    <h4>Tasks</h4>

    <table class="table table-striped">
        <colgroup>
            <col width="*">
            <col width="120">
            <col width="100">
        </colgroup>
      <g:each in="${activeMigration?.taskList}" var="task" status="i">
        <tr class="row-condensed">
            <td class="click-detail" data-content="${task.description}" data-container="body" data-placement="auto top">
                ${task.name}<g:if test="${task.taskMessage}"><br>
                <span class="minor"><em>Message: </em>${raw(task.taskMessage)}</span></g:if>
            </td>
            <td><div class="progress"><g:set var="progressBarType" value="${[New: 'info', Running: 'info', Success: 'success', Error: 'danger'][task.taskStatus as String]}" />
                <div class="progress-bar progress-bar-${progressBarType} progress-bar-striped" role="progressbar"
                     aria-valuenow="${task.percentComplete}" aria-valuemin="0" aria-valuemax="100" style="width:${task.percentComplete}%">
                    ${task.percentComplete}% Complete
                </div>
            </div></td>
            <td>${task.taskStatus}</td></tr>
      </g:each>
    </table>

<script>
    $(document).ready(function () {
        setInterval(function () {
            if (!openPopovers()) window.location.reload();
        }, 5000); // refresh 5 seconds unless popovers are open
    });
</script>

</body>
</html>
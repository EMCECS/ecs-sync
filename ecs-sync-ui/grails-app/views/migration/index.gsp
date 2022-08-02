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

<h2>Migrations</h2>
<table class="table table-striped">
    <g:if test="${migrationEntries}">
        <tr><th>Name</th><th>Status</th><th>&nbsp;</th><th>&nbsp;</th></tr>
        <g:each in="${migrationEntries}" var="entry">
            <g:set var="activeMigration" value="${activeMigrations.find { it.migrationConfig == entry.migrationConfig }}" />
            <tr><td><g:link action="show" params="[migrationKey: entry.xmlKey]"><strong>${entry.type.capitalize()}</strong>: ${entry.migrationConfig.description}</g:link></td>
                <td><g:if test="${activeMigration}"><div class="progress">
                    <div class="progress-bar progress-bar-info progress-bar-striped" role="progressbar"
                         aria-valuenow="${activeMigration.percentComplete()}" aria-valuemin="0" aria-valuemax="100" style="width:${activeMigration.percentComplete()}%">
                        ${activeMigration.percentComplete()}% Complete
                    </div>
                </div></g:if></td>
                <td><g:if test="${activeMigration}">${activeMigration.state}
                  </g:if><g:else>WARNING: this migration is not active
                </g:else></td>
                <td><g:if test="${activeMigration}"><g:if test="${activeMigration?.isRunning()}"><g:link action="stop" params="[migrationKey: entry.xmlKey]" class="btn btn-sm btn-danger"
                          onclick="return confirm('Are you sure you want to stop this migration: ${entry.migrationConfig.description}')">x</g:link></g:if>
                    <g:if test="${activeMigration?.isPaused()}"><g:link action="resume" params="[migrationKey: entry.xmlKey]" class="btn btn-sm btn-info"
                          onclick="return confirm('Are you sure you want to resume this migration: ${entry.migrationConfig.description}')">&#x25ba;</g:link></g:if>
                    <g:if test="${activeMigration?.isStopped()}"><g:link action="archive" params="[migrationKey: entry.xmlKey]" class="btn btn-sm btn-warning"
                          title="Archive (the information here is available in the migration report)"
                          onclick="return confirm('Are you sure you want to archive this migration: ${entry.migrationConfig.description}')">&#x2713</g:link></g:if>
                  </g:if><g:else>
                    <g:link action="restart" params="[migrationKey: entry.xmlKey]" class="btn btn-sm btn-warning"
                            title="Restart this migration"
                            onclick="return confirm('Are you sure you want to restart this migration?')">&#x21bb;</g:link>
                </g:else></td></tr>
        </g:each>
    </g:if><g:else>
        <tr><td>No migrations initiated</td></tr>
    </g:else>
</table>

<hr>
<h4>NOTE: Currently ecs-sync <em>only</em> supports Atmos-to-ECS <strong>S3</strong> migrations. This does <strong>not</strong> migrate Atmos namespace or objectspace data!</h4>
<p>
    <g:link action="create" params="['type': 'a2e']" class="btn btn-primary">Start A2E Migration</g:link>
</p>

</body>
</html>
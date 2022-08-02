%{--
  - Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
<%@ page import="sync.ui.ScheduledSync; org.springframework.validation.FieldError" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>
<h2>Edit Sync Schedule</h2>

<g:if test="${flash.message}">
    <div class="alert alert-info" role="status">${flash.message}</div>
</g:if>
<g:if test="${scheduleEntry.hasErrors() || scheduleEntry.scheduledSync.hasErrors()}">
    <div class="alert alert-danger">
        <ul class="errors" role="alert">
            <g:eachError bean="${scheduleEntry}" var="error">
                <li <g:if test="${error in FieldError}">data-field-id="${error.field}"</g:if>><g:message
                          error="${error}"/></li>
            </g:eachError>
            <g:eachError bean="${scheduleEntry.scheduledSync}" var="error">
                <li <g:if test="${error in FieldError}">data-field-id="${error.field}"</g:if>><g:message
                        error="${error}"/></li>
            </g:eachError>
        </ul>
    </div>
</g:if>

<g:form action="update" method="PUT" class="disable-enter">
    <g:hiddenField name="scheduleEntry.old_name" value="${scheduleEntry.name}" />
    <table class="table table-striped table-condensed kv-table">
        <tr><th>Name:</th><td><g:textField name="scheduleEntry.name" value="${scheduleEntry.name}"/></td></tr>
        <tr><th>Run On:</th><td><table class="table-bordered table-condensed text-center">
            <tr><g:each in="${ScheduledSync.Day.values()}"><td>${it}</td></g:each></tr>
            <tr><g:each in="${ScheduledSync.Day.values()}"><td>
                <g:checkBox name="scheduleEntry.scheduledSync.daysOfWeek" value="${it}"
                            checked="${scheduleEntry.scheduledSync.daysOfWeek.contains(it)}" />
            </td></g:each></tr>
        </table></td></tr>
        <tr><th>Start Time:</th><td>
            <g:select name="scheduleEntry.scheduledSync.startHour" from="${0L..23L}" value="${scheduleEntry.scheduledSync.startHour}" /> :
            <g:select name="scheduleEntry.scheduledSync.startMinute" from="${(0L..11L)*.multiply(5)}" value="${scheduleEntry.scheduledSync.startMinute}" />
        </td></tr>
        <tr><th>Alert On: </th><td>
            <g:checkBox name="scheduleEntry.scheduledSync.alerts.onStart" value="${scheduleEntry.scheduledSync.alerts.onStart}" /> Start&nbsp;&nbsp;&nbsp;&nbsp;
            <g:checkBox name="scheduleEntry.scheduledSync.alerts.onComplete" value="${scheduleEntry.scheduledSync.alerts.onComplete}" /> Complete&nbsp;&nbsp;&nbsp;&nbsp;
            <g:checkBox name="scheduleEntry.scheduledSync.alerts.onError" value="${scheduleEntry.scheduledSync.alerts.onError}" /> Error <small>(including transfer errors)</small>
        </td></tr>
        <tr><th>Precheck Script:</th><td>
            <g:textField name="scheduleEntry.scheduledSync.preCheckScript" value="${scheduleEntry.scheduledSync.preCheckScript}" size="60" /> <br>
            <small>(Job will not be scheduled if script returns non-zero exit value)</small>
        </td></tr>
    </table>

    <g:render template="/sync/syncConfig"
              model="[uiConfig: uiConfig, syncConfig: scheduleEntry.scheduledSync.config, prefix: 'scheduleEntry.scheduledSync.config']"/>

    <g:submitButton name="update" value="Update" class="btn btn-primary"/>
</g:form>

</body>
</html>
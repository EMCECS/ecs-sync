<%@ page import="sync.ui.ConfigStorageType; org.springframework.validation.FieldError" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
    <script>$(function () {
        changeConfigStorage($('input[name=configStorageType]:checked'));
    });</script>
</head>

<body>

<g:if test="${flash.message}">
    <div class="alert alert-info" role="status">${flash.message}</div>
</g:if>
<g:if test="${request.exception}">
    <div class="alert alert-danger" role="alert">${request.exception}</div>
</g:if>
<g:hasErrors bean="${this.uiConfig}">
    <div class="alert alert-danger">
        <ul class="errors" role="alert">
            <g:eachError bean="${this.uiConfig}" var="error">
                <li <g:if test="${error in FieldError}">data-field-id="${error.field}"</g:if>><g:message error="${error}"/></li>
            </g:eachError>
        </ul>
    </div>
</g:hasErrors>

<g:form action="save" method="POST" class="disable-enter">

<h2>Storage</h2>
<p>
    Storage location for UI configuration, schedules and reports
</p>
<table class="table table-striped table-condensed kv-table">
    <tr><td><g:radioGroup name="configStorageType" values="${ConfigStorageType.values()}" labels="${ConfigStorageType.values()}"
                          value="${uiConfig.configStorageType}" onchange="changeConfigStorage(this)">
        ${it.radio} ${it.label} &nbsp;&nbsp;&nbsp;&nbsp;</g:radioGroup></td></tr>
</table>

<table class="table table-striped table-condensed kv-table" id="storage-configuration">
    <tbody data-storage-type="LocalDisk">
    <tr><th>Base Path: </th><td><g:textField name="filePath" value="${uiConfig.filePath}" size="60" /></td></tr>
    <tr><td><g:submitButton name="readConfig" value="Read Configuration from Disk" class="btn btn-primary" /></td>
        <td>&nbsp;</td></tr>
    </tbody>

    <tbody data-storage-type="ECS">
    <tr><th>ECS Endpoints: </th><td><g:textField name="hosts" value="${uiConfig.hosts}" size="60" /><br/>
        <span style="color: gray">host1[,host2]</span></td></tr>
    <tr class="advanced"><th>Protocol: </th><td><g:radioGroup name="protocol" values="${['HTTP','HTTPS']}" labels="${['HTTP','HTTPS']}" value="${uiConfig.protocol}">
        ${it.radio} ${it.label} &nbsp;&nbsp;&nbsp;&nbsp;</g:radioGroup></td></tr>
    <tr class="advanced"><th>Port: </th><td><g:textField name="port" value="${uiConfig.port}" size="40" /></td></tr>
    <tr><th>ECS User: </th><td><g:textField name="accessKey" value="${uiConfig.accessKey}" size="40" /></td></tr>
    <tr><th>ECS Secret: </th><td><g:passwordField name="secretKey" value="${uiConfig.secretKey}" size="60" />
        <span class="passwordToggle" data-target-id="secretKey">show</span></td></tr>
    <tr class="advanced"><th>Config Bucket: </th><td><g:textField name="configBucket" value="${uiConfig.configBucket}" /></td></tr>
    <tr><td><g:submitButton name="readConfig" value="Read Configuration from ECS" class="btn btn-primary" /></td>
        <td><a class="toggle-advanced" href="#" onclick="toggleAdvanced(document.getElementById('storage-configuration')); return false;">show advanced options</a></td></tr>
    </tbody>
</table>

<h2>Options</h2>
<table class="table table-striped table-condensed kv-table">
    <tr><th>Auto Archive:</th>
        <td><g:checkBox name="autoArchive" value="${uiConfig.autoArchive}" /> <small>(automatically archive completed syncs)</small></td></tr>
    <tr><th>Alert Email:</th>
        <td><g:textField name="alertEmail" value="${uiConfig.alertEmail}" /><br>
            <small>(all alerts will be sent to this address)</small></td></tr>
</table>

<g:submitButton name="writeConfig" value="Save & Write Configuration to Storage" class="btn btn-primary" />

</g:form>

</body>
</html>
<%@ page import="org.springframework.validation.FieldError" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
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

<g:form resource="${uiConfig}" action="update" method="PUT">

<h2>ECS Configuration</h2>
<table class="table table-striped table-condensed kv-table">
    <tr><th>ECS Endpoints: </th><td><g:textField name="hosts" value="${uiConfig.hosts}" size="60" /><br/>
        <span style="color: gray">host1[,host2]</span></td></tr>
    <tr class="advanced"><th>Protocol: </th><td><g:radioGroup name="protocol" values="${['HTTP','HTTPS']}" labels="${['HTTP','HTTPS']}" value="${uiConfig.protocol}">
        ${it.radio} ${it.label} &nbsp;&nbsp;&nbsp;&nbsp;</g:radioGroup></td></tr>
    <tr class="advanced"><th>Port: </th><td><g:textField name="port" value="${uiConfig.port}" size="40" /></td></tr>
    <tr><th>ECS User: </th><td><g:textField name="accessKey" value="${uiConfig.accessKey}" size="40" /></td></tr>
    <tr><th>ECS Secret: </th><td><g:passwordField name="secretKey" value="${uiConfig.secretKey}" size="60" />
        <span class="passwordToggle" data-target-id="secretKey">show</span></td></tr>
    <tr class="advanced"><th>Config Bucket: </th><td><g:textField name="configBucket" value="${uiConfig.configBucket}" /></td></tr>
    <tr><td><g:submitButton name="readEcs" value="Read Configuration from ECS" class="btn btn-primary" /></td>
        <td><a href="#" onclick="toggleAdvanced($(this))">show advanced options</a></td></tr>
</table>

<h2>Defaults</h2>
<table class="table table-striped table-condensed kv-table">
    <tr><th>Filesystem Source Directory</th>
        <td><g:textField name="defaults[source.FilesystemSource.rootFile]" value="${uiConfig.defaults['source.FilesystemSource.rootFile']}" size="80" /></td></tr>
    <tr><th>Ecs S3 Target Bucket</th>
        <td><g:textField name="defaults[target.EcsS3Target.bucketName]" value="${uiConfig.defaults['target.EcsS3Target.bucketName']}" size="40" /></td></tr>
    <tr><td colspan="2">&nbsp;</td></tr>
    <tr><th>Ecs S3 Source Bucket</th>
        <td><g:textField name="defaults[source.EcsS3Source.bucketName]" value="${uiConfig.defaults['source.EcsS3Source.bucketName']}" size="40" /></td></tr>
    <tr><th>Filesystem Target Directory</th>
        <td><g:textField name="defaults[target.FilesystemTarget.targetRoot]" value="${uiConfig.defaults['target.FilesystemTarget.targetRoot']}" size="80" /></td></tr>
    <tr><td colspan="2">&nbsp;</td></tr>
    <tr><th>Sync Thread Count</th><td><g:select name="defaults[syncThreadCount]" from="${["16","24","32","40","48","64","80","96","128"]}" value="${uiConfig.defaults['syncThreadCount']}" /></td></tr>
    <tr><th>Query Thread Count</th><td><g:select name="defaults[queryThreadCount]" from="${["16","24","32","40","48","64","80","96","128"]}" value="${uiConfig.defaults['queryThreadCount']}" /></td></tr>
</table>

<h2>Options</h2>
<table class="table table-striped table-condensed kv-table">
    <tr><th>Auto Archive:</th>
        <td><g:checkBox name="autoArchive" value="${uiConfig.autoArchive}" /> <small>(automatically archive completed syncs)</small></td></tr>
    <tr><th>Alert Email:</th>
        <td><g:textField name="alertEmail" value="${uiConfig.alertEmail}" /><br>
            <small>(all alerts will be sent to this address)</small></td></tr>
</table>

<g:submitButton name="writeEcs" value="Save & Write Configuration to ECS" class="btn btn-primary" />

</g:form>

</body>
</html>
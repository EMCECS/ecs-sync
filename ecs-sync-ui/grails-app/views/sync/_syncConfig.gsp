<%@ page import="org.springframework.validation.FieldError" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <script>$(function () {
        changePlugin('Source', document.getElementById('${prefix}.source.pluginClass'));
        changePlugin('Target', document.getElementById('${prefix}.target.pluginClass'));
    });</script>
</head>

<body>
<g:set var="sourcePlugins" value="[
        'com.emc.ecs.sync.source.FilesystemSource',
        'com.emc.ecs.sync.source.EcsS3Source'
]"/>
<g:set var="targetPlugins" value="[
        'com.emc.ecs.sync.target.EcsS3Target',
        'com.emc.ecs.sync.target.FilesystemTarget'
]"/>

<g:if test="${flash.error}">
    <div class="alert alert-danger" role="alert">${flash.error}</div>
</g:if>

<div class="panel panel-default">
    <div class="panel-heading">
        <h3 class="panel-title">Source:
        <g:select name="${prefix}.source.pluginClass" from="${sourcePlugins.collect { it.split('\\.').last() }}"
                  keys="${sourcePlugins}" value="${syncConfig.source?.pluginClass}"
                  onchange="changePlugin('Source', this)"/>
        </h3>
    </div>

    <div class="panel-body">
        <table class="table table-striped table-condensed kv-table" data-plugin="Source">
            <tbody data-plugin="FilesystemSource">
            <tr>
                <th>Root Folder</th>
                <td><g:textField size="40" name="${prefix}.source.customProperties.rootFile"
                                 value="${syncConfig.source?.customProperties?.rootFile ?: uiConfig.defaults['source.FilesystemSource.rootFile']}"/></td>
            </tr>
            </tbody>

            <tbody data-plugin="EcsS3Source">
            <tr style="display: none"><td>
                <g:hiddenField name="${prefix}.source.customProperties.protocol" value="${uiConfig.protocol}"/>
                <g:hiddenField name="${prefix}.source.customListProperties.vdcs" value="${uiConfig.hosts}"/>
                <g:hiddenField name="${prefix}.source.customProperties.port" value="${uiConfig.port}"/>
                <g:hiddenField name="${prefix}.source.customProperties.accessKey" value="${uiConfig.accessKey}"/>
                <g:hiddenField name="${prefix}.source.customProperties.secretKey" value="${uiConfig.secretKey}"/>
            </td></tr>
            <tr>
                <th>Bucket</th>
                <td><g:textField size="40" name="${prefix}.source.customProperties.bucketName"
                                 value="${syncConfig.source?.customProperties?.bucketName ?: uiConfig.defaults['source.EcsS3Source.bucketName']}"/></td>
            </tr>
            <tr class="advanced">
                <th>Use Apache HTTP Client?</th>
                <td><input type="checkbox" name="${prefix}.source.customProperties.apacheClientEnabled"
                           title="apacheClientEnabled" ${syncConfig.source?.customProperties?.apacheClientEnabled ? 'checked="checked"' : ''}/>
                </td>
            </tr>
            </tbody>
        </table>
    </div>
</div>

<hr>

<div class="panel panel-default">
    <div class="panel-heading">
        <h3 class="panel-title">Target:
        <g:select name="${prefix}.target.pluginClass" from="${targetPlugins.collect { it.split('\\.').last() }}"
                  keys="${targetPlugins}" value="${syncConfig.target?.pluginClass}"
                  onchange="changePlugin('Target', this)"/>
        </h3>
    </div>

    <div class="panel-body">
        <table class="table table-striped table-condensed kv-table" data-plugin="Target">
            <tbody data-plugin="EcsS3Target">
            <tr style="display: none"><td>
                <g:hiddenField name="${prefix}.target.customProperties.protocol" value="${uiConfig.protocol}"/>
                <g:hiddenField name="${prefix}.target.customListProperties.vdcs" value="${uiConfig.hosts}"/>
                <g:hiddenField name="${prefix}.target.customProperties.port" value="${uiConfig.port}"/>
                <g:hiddenField name="${prefix}.target.customProperties.accessKey" value="${uiConfig.accessKey}"/>
                <g:hiddenField name="${prefix}.target.customProperties.secretKey" value="${uiConfig.secretKey}"/>
            </td></tr>
            <tr>
                <th>Bucket</th>
                <td><g:textField size="40" name="${prefix}.target.customProperties.bucketName"
                                 value="${syncConfig.target?.customProperties?.bucketName ?: uiConfig.defaults['target.EcsS3Target.bucketName']}"/></td>
            </tr>
            <tr class="advanced">
                <th>Create Bucket?</th>
                <td><input type="checkbox" name="${prefix}.target.customProperties.createBucket" title="createBucket"
                           checked="checked"/></td>
            </tr>
            <tr class="advanced">
                <th>Disable MPU?</th>
                <td><input type="checkbox" name="${prefix}.target.customProperties.mpuDisabled" title="mpuDisabled"
                           checked="checked"/></td>
            </tr>
            <tr class="advanced">
                <th>Use Apache HTTP Client?</th>
                <td><input type="checkbox" name="${prefix}.target.customProperties.apacheClientEnabled"
                           title="apacheClientEnabled"/></td>
            </tr>
            </tbody>

            <tbody data-plugin="FilesystemTarget">
            <tr>
                <th>Target Folder</th>
                <td><g:textField size="40" name="${prefix}.target.customProperties.targetRoot"
                                 value="${syncConfig.target?.customProperties?.rootFile ?: uiConfig.defaults['target.FilesystemTarget.targetRoot']}"/></td>
            </tr>
            <tr class="advanced">
                <th>Exclude Paths<br/>
                    <small>(<a target="_blank"
                               href="https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html"
                               style="display: inline">regex patterns</a> supported)</small></th>
                <td><g:textArea name="${prefix}.target.customProperties.excludePaths"
                                value="${syncConfig.target?.customProperties?.excludePaths}"/></td>
            </tr>
            </tbody>
        </table>
    </div>
</div>

<table class="table table-striped table-condensed kv-table">
    <tr><th>Query Thread Count</th>
        <td><g:select name="${prefix}.queryThreadCount" from="${["16", "24", "32", "40", "48", "64"]}"
                      value="${syncConfig.queryThreadCount ?: uiConfig.defaults['queryThreadCount']}"/></td>
    </tr>
    <tr><th>Sync Thread Count</th>
        <td><g:select name="${prefix}.syncThreadCount" from="${["16", "24", "32", "40", "48", "64", "80", "96", "128"]}"
                      value="${syncConfig.syncThreadCount ?: uiConfig.defaults['syncThreadCount']}"/></td>
    </tr>
    <tr class="advanced">
        <th>Split Pools Threshold</th>
        <g:set var="sptOpts" value="${[1,2,4,6,10]}" />
        <td><g:select name="${prefix}.splitPoolsThreshold" from="${['Disabled'] + sptOpts.collect { "${it}MB" }}"
            keys="${[''] + sptOpts.collect {it * 1024 * 1024}}" value="${syncConfig.splitPoolsThreshold}"/></td>
    </tr>
    <tr class="advanced"><th>Monitor Performance?</th><td><g:checkBox name="${prefix}.monitorPerformance"
                                                                      value="${true}"/></td>
    </tr>
    <tr class="advanced"><th>Log Level</th>
        <td><g:radioGroup name="${prefix}.logLevel" values="['debug', 'verbose', 'quiet', 'silent']"
                          labels="['debug', 'verbose', 'quiet', 'silent']" value="${syncConfig.logLevel ?: 'quiet'}">
            ${it.radio} ${it.label} &nbsp;&nbsp;&nbsp;&nbsp;</g:radioGroup></td></tr>
    <tr class="advanced"><th>DB Type</th>
        <td><g:radioGroup name="${prefix}.dbType" values="['Sqlite','mySQL','None']"
                          labels="['Sqlite','mySQL','None']" value="${grailsApplication.config.sync.defaultDb}">
            ${it.radio} ${it.label} &nbsp;&nbsp;&nbsp;&nbsp;</g:radioGroup></td></tr>
</table>

<p>
    <a href="#" onclick="toggleAdvanced($(this))">show advanced options</a>
</p>

</body>
</html>

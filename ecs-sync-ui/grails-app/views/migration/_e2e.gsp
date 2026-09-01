%{--
  - Copyright (c) 2024 Dell Inc. or its subsidiaries. All Rights Reserved.
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
<body>

<g:if test="${flash.error}">
    <div class="alert alert-danger" role="alert">${flash.error}</div>
</g:if>

<p><em>Copies namespaces, object users, buckets and all object data from one ECS to another. If a namespace name is
already in use on the target, it is created with a suffix and its object users are renamed to match &mdash; which
means the source and target may be the <strong>same</strong> cluster, to migrate between storage pools.</em></p>

<table class="table table-striped table-condensed kv-table">
    <tr><th>Source ECS:</th>
        <td><g:select name="${prefix}.sourceXmlKey" from="${sourceEntries}"
                      noSelection="${['': '--- Select One ---']}" optionKey="xmlKey" optionValue="name"
                      value="${fieldValue(bean: bean, field: 'sourceXmlKey')}"/></td>
        <td><div id="sourceTestResult" class="testResult"></div></td></tr>
    <tr><th>Target ECS:</th>
        <td><g:select name="${prefix}.targetXmlKey" from="${targetEntries}"
                      noSelection="${['': '--- Select One ---']}" optionKey="xmlKey" optionValue="name"
                      value="${fieldValue(bean: bean, field: 'targetXmlKey')}"/>
            <span class="plugin-info glyphicon glyphicon-info-sign"
                  data-content="Select the same ECS as the source to migrate between storage pools within one
                                cluster. Every namespace will then be recreated under a new name."></span></td>
        <td><div id="targetTestResult" class="testResult"></div></td></tr>
    <tr><th>Target Replication Group:</th>
        <td><select id="${prefix}.replicationGroup" name="${prefix}.replicationGroup"></select>
            <span class="plugin-info glyphicon glyphicon-info-sign"
                  data-content="Determines which storage pool receives the data. Namespaces and buckets created by
                                this migration are placed in this replication group."></span></td>
        <td><div id="replicationGrpTestResult" class="testResult"></div></td></tr>
    <tr><th>Target Namespace Suffix:</th>
        <td><g:textField name="${prefix}.targetNamespaceSuffix"
                         value="${fieldValue(bean: bean, field: 'targetNamespaceSuffix')}"/>
            <span class="plugin-info glyphicon glyphicon-info-sign"
                  data-content="Appended when a namespace name is already taken on the target (ns1 becomes
                                ns1_target). A counter is added if that is taken too (ns1_target2). Object users in a
                                renamed namespace are renamed with the same suffix, because ECS can be configured with
                                GLOBAL user scope where object user IDs must be unique cluster-wide."></span></td>
        <td></td></tr>
    <tr><th>Threads per Bucket:</th>
        <td><g:textField name="${prefix}.threadCount" value="${fieldValue(bean: bean, field: 'threadCount')}"/></td>
        <td></td></tr>
    <tr><th>Verify:</th>
        <td><g:checkBox name="${prefix}.verify" value="${bean.verify}"/>
            <span class="plugin-info glyphicon glyphicon-info-sign"
                  data-content="Read back and compare each object after it is written."></span></td>
        <td></td></tr>
    <tr><th>Include Versions:</th>
        <td><g:checkBox name="${prefix}.includeVersions" value="${bean.includeVersions}"/>
            <span class="plugin-info glyphicon glyphicon-info-sign"
                  data-content="Copy all versions of objects in versioned buckets, rather than only the current
                                version."></span></td>
        <td></td></tr>
    <tr><th>Generate Source Secret Keys:</th>
        <td><g:checkBox name="${prefix}.generateSourceSecretKeys" value="${bean.generateSourceSecretKeys}"/>
            <span class="plugin-info glyphicon glyphicon-info-sign"
                  data-content="The management API never discloses an existing user's secret key. When enabled, a key
                                is generated on the SOURCE cluster for any user that has none, so its data can be
                                read. ECS supports two keys per user, so existing keys are not disturbed - but this
                                does modify the source cluster. Disable to fail instead."></span></td>
        <td></td></tr>
    <tr><th>Trust Self-Signed Certificates:</th>
        <td><g:checkBox name="${prefix}.trustSelfSignedCertificates" value="${bean.trustSelfSignedCertificates}"/>
            <span class="plugin-info glyphicon glyphicon-info-sign"
                  data-content="ECS management endpoints commonly present a self-signed certificate. This disables
                                certificate verification for management API calls only."></span></td>
        <td></td></tr>
</table>

<div id="namespaceTestResult" class="testResult" style="float: right; width: 80%"></div>

<h3>Namespaces</h3>
<p><em>Select the namespaces to migrate. All object users and buckets within each are included.</em></p>

<table class="table table-striped table-condensed">
    <colgroup><col width="40"><col width="300"><col width="*"></colgroup>
    <thead><tr><th><input type="checkbox" onchange="selectAllNamespaces(this)"></th>
               <th>Source Namespace</th><th>&nbsp;</th></tr></thead>
    <tbody id="namespaceTableBody"></tbody>
</table>

<script id="template-namespaceRow" type="text/html">
<tr><td class="namespaceCheckbox"><input type="checkbox"></td>
    <td class="namespaceName"></td>
    <td><div class="testResult"></div></td></tr>
</script>

<script>
    $(document).ready(function () {
        var selectedNamespaces = [<g:each in="${bean.selectedNamespaces}" var="ns" status="i"><g:if test="${i > 0}">,</g:if>'${ns}'</g:each>];

        $("select[name='${prefix}.sourceXmlKey']").change(function () {
            var $body = $("#namespaceTableBody");
            $body.html(''); // clear table
            var val = $(this).val();
            if (!val) return;
            $.ajax({
                url: '<g:createLink controller="ecs" action="listNamespaces"/>',
                data: 'storageKey=' + val,
                dataType: 'json',
                success: function (json) {
                    var i = 0;
                    $.each(json, function (idx, value) {
                        var $row = $($('#template-namespaceRow').html());
                        $row.find('.namespaceName').text(value.name);
                        var $checkbox = $row.find('.namespaceCheckbox input');
                        $checkbox.attr('id', '${prefix}.selectedNamespaces[' + i + ']');
                        $checkbox.attr('name', '${prefix}.selectedNamespaces[' + i + ']');
                        $checkbox.val(value.name);
                        if ($.inArray(value.name, selectedNamespaces) >= 0) $checkbox.prop('checked', true);
                        $row.find('.testResult').attr('id', 'nsTestResult[' + value.name + ']');
                        $body.append($row);
                        i++;
                    });
                },
                error: function (request) {
                    $body.append($('<tr><td colspan="3" class="text-danger"></td></tr>')
                        .find('td').text('Could not list namespaces: ' + request.responseText).end());
                }
            });
        });

        $("select[name='${prefix}.targetXmlKey']").change(function () {
            var $target = $("select[name='${prefix}.replicationGroup']");
            $target.html(''); // clear options
            var val = $(this).val();
            if (!val) return;
            $.ajax({
                url: '<g:createLink controller="ecs" action="listReplicationGroups"/>',
                data: 'storageKey=' + val,
                dataType: 'json',
                success: function (json) {
                    $.each(json.vpools, function (i, value) {
                        // the resolver matches on name, falling back to id
                        var $option = $('<option>').text(value.name).attr('value', value.name);
                        if (value.name === '${fieldValue(bean: bean, field: 'replicationGroup')}')
                            $option.attr('selected', 'selected');
                        $target.append($option);
                    });
                }
            });
        });

        $("select[name='${prefix}.sourceXmlKey']").change();
        $("select[name='${prefix}.targetXmlKey']").change();
    });

    function selectAllNamespaces(checkbox) {
        $('.namespaceCheckbox input').prop('checked', checkbox.checked);
    }

    // called by create.gsp when the Test button returns
    function migrationTestComplete(data) {
        function show(id, result) {
            if (!result) return;
            $('#' + id).attr('class', 'testResult text-' + (result.status === 'danger' ? 'danger'
                    : result.status === 'warning' ? 'warning' : 'success'))
                    .text(result.message).show();
        }

        show('sourceTestResult', data.source);
        show('targetTestResult', data.target);
        show('replicationGrpTestResult', data.replicationGrp);
        show('namespaceTestResult', data.sameCluster);

        $.each(data.namespaces || {}, function (namespace, result) {
            var $cell = $('[id="nsTestResult[' + namespace + ']"]');
            $cell.attr('class', 'testResult text-' + (result.status === 'danger' ? 'danger'
                    : result.status === 'warning' ? 'warning' : 'success')).text(result.message);
        });
    }
</script>

</body>
</html>

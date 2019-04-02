<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<body>

<g:if test="${flash.error}">
    <div class="alert alert-danger" role="alert">${flash.error}</div>
</g:if>

<table class="table table-striped table-condensed kv-table">
    <tr><th>Source:</th>
        <td><g:select name="${prefix}.sourceXmlKey" from="${sourceEntries}"
                      noSelection="${['': '--- Select One ---']}" optionKey="xmlKey" optionValue="name"
                      value="${fieldValue(bean: bean, field: 'sourceXmlKey')}"/></td>
        <td></td></tr>
    <tr><th>Source Subtenant Name:</th>
        <td><select id="${prefix}.atmosSubtenantName"
                    name="${prefix}.atmosSubtenantName"></select></td>
        <td><div id="atmosSubtenantTestResult" class="testResult"></div></td></tr>
    <tr><th>Target:</th>
        <td><g:select name="${prefix}.targetXmlKey" from="${targetEntries}"
                      noSelection="${['': '--- Select One ---']}" optionKey="xmlKey" optionValue="name"
                      value="${fieldValue(bean: bean, field: 'targetXmlKey')}"/></td>
        <td></td></tr>
    <tr><th>Target Namespace Name:</th><td><input type="text" id="ecsNamespace" name="ecsNamespace">
            <button class="btn btn-primary" onclick="setTargetNamespace()">Set</button></td>
        <td></td></tr>
    <tr><th>Target Replication Group:<br/>(for new namespaces)</th>
        <td><select id="${prefix}.replicationGroup"
                    name="${prefix}.replicationGroup"></select>
            <span class="plugin-info glyphicon glyphicon-info-sign"
                  data-content="This replication group will be the default for any namespaces created by the migration
                                job. If the default RG will be different for certain namespaces, you must manually
                                create those namespaces (with the appropriate default RG) prior to the migration."></span></td>
        <td></td></tr>
    <tr><th>Stale MPU Threshold (days):</th>
        <td><g:textField name="${prefix}.staleMpuThresholdDays"
                         value="${fieldValue(bean: bean, field: 'staleMpuThresholdDays')}"/>
            <span class="plugin-info glyphicon glyphicon-info-sign"
                  data-content="Existing MPUs must be completed prior to starting a bucket migration. Each bucket will
                                be checked for incomplete MPUs. Any incomplete MPUs older than this threshold will be
                                considered stale, and ignored. If any are found that are younger than this threshold,
                                and error is generated, which will pause the migration and allow manual intervention.
                                Once you have determined these MPUs are complete or are abandoned, it is safe to resume
                                the migration."></span></td>
        <td></td></tr>
</table>

<script>
    $(document).ready(function () {
        $("select[name='${prefix}.sourceXmlKey']").change(function () {
            var $target = $("select[name='${prefix}.atmosSubtenantName']");
            $target.html(''); // clear options
            var val = $(this).val();
            if (val) {
                $.ajax({
                    url: '<g:createLink controller="atmos" action="listSubtenants"/>',
                    data: 'storageKey=' + val,
                    dataType: 'json',
                    success: function (json) {
                        $.each(json, function (i, value) {
                            var $option = $('<option>').text(value.name).attr('value', value.name);
                            if (value.name === '${fieldValue(bean:bean,field:'atmosSubtenantName')}') $option.attr('selected', 'selected');
                            $target.append($option);
                        });
                    }
                });
            }
            $target.change();
        });

        $("select[name='${prefix}.atmosSubtenantName']").change(function () {
            var subtenantVal = $(this).val();
            // show upload button
            if (subtenantVal) $('#userMapUploadButton').show();
            // default the namespace
            var $namespace = $("#ecsNamespace");
            $namespace.val(subtenantVal);
            // populate users
            var selectedUsers = [${bean.userMap.keySet}];
            var storageKey = $("select[name='${prefix}.sourceXmlKey']").val();
            var $target = $("#userTableBody");
            $target.html(''); // clear table
            if (subtenantVal) {
                $.ajax({
                    url: '<g:createLink controller="atmos" action="getSubtenant"/>',
                    data: 'storageKey=' + storageKey + '&subtenantName=' + subtenantVal,
                    dataType: 'json',
                    success: function (json) {
                        var i = 0;
                        $.each(json.objectUsers, function (idx, value) {
                            if (!value.status || value.status.name !== 'Operational') return;
                            var $row = $($('#template-userMapRow').html());
                            $row.find('.atmosUid').text(value.uid);
                            var $userCheckbox = $row.find('.userCheckbox input');
                            $userCheckbox.attr('id', '${prefix}.selectedUsers[' + i + ']');
                            $userCheckbox.attr('name', '${prefix}.selectedUsers[' + i + ']');
                            $userCheckbox.val(value.uid);
                            $row.find('.ecsUser input').each(function () {
                                var $this = $(this);
                                $this.attr('id', '${prefix}.userMap[' + value.uid + ']');
                                $this.attr('name', '${prefix}.userMap[' + value.uid + ']');
                                $this.val(value.uid);
                            });
                            $row.find('.ecsNamespace input').each(function () {
                                var $this = $(this);
                                $this.attr('id', '${prefix}.userNamespaceMap[' + value.uid + ']');
                                $this.attr('name', '${prefix}.userNamespaceMap[' + value.uid + ']');
                                $this.val(subtenantVal);
                            });
                            $row.find('.testResult').attr('id', 'testResult[' + value.uid + ']');
                            $target.append($row);
                            if ($.inArray(value.uid, selectedUsers))
                                $row.find('.userCheckbox').prop("checked", true);
                            i++;
                        });
                    }
                });
            }
        });

        $("select[name='${prefix}.targetXmlKey']").change(function () {
            var $target = $("select[name='${prefix}.replicationGroup']");
            $target.html(''); // clear options
            var val = $(this).val();
            if (val) {
                $.ajax({
                    url: '<g:createLink controller="ecs" action="listReplicationGroups"/>',
                    data: 'storageKey=' + val,
                    dataType: 'json',
                    success: function (json) {
                        $.each(json.vpools, function (i, value) {
                            var $option = $('<option>').text(value.name).attr('value', value.id);
                            if (value.id === '${fieldValue(bean:bean,field:'replicationGroup')}') $option.attr('selected', 'selected');
                            $target.append($option);
                        });
                    }
                });
            }
        });

        $("select[name='${prefix}.sourceXmlKey']").change();
        $("select[name='${prefix}.targetXmlKey']").change();
    });

    function selectAllUsers(checkbox) {
        $('.userCheckbox input').prop('checked', checkbox.checked);
    }
</script>

<div id="userMapUpload">
    <button id="userMapUploadButton" class="btn btn-primary">Upload Mapping CSV</button>
    <input type="file" name="userMapFile" id="userMapFile" style="visibility: hidden;" disabled="disabled">
</div>

<div id="userMapTestResult" class="testResult" style="float: right; width: 80%"></div>

<h3>Users</h3>
<p><em>Note: if a user mapping has already been established for this subtenant, that will override any mapped values here.</em></p>

<table class="table table-striped table-condensed">
    <colgroup><col width="40"><col width="200"><col width="200"><col width="200"><col width="*"></colgroup>
    <thead><tr><th><input type="checkbox" onchange="selectAllUsers(this)"></th><th>Source User</th><th>Target User</th>
               <th>Target Namespace</th><th>&nbsp;</th></tr></thead>
    <tbody id="userTableBody"></tbody>
</table>

<script id="template-userMapRow" type="text/html">
<tr><td class="userCheckbox"><input type="checkbox"></td>
    <td class="atmosUid"></td>
    <td class="ecsUser"><input type="text" required></td>
    <td class="ecsNamespace"><input type="text" required></td>
    <td><div class="testResult"></div></td></tr>
</script>

<div id="s3ProxyTestResult" class="testResult" style="float: right; width: 80%"></div>

<h3>S3 Proxy</h3>
<table class="table table-striped table-condensed kv-table">
    <tr><th>Use S3 Proxy</th><td>
        <g:checkBox name="${prefix}.useS3Proxy" value="${bean.useS3Proxy}"/></td></tr>
    <g:if test="${config.protocol && config.hosts && config.port}"
      ><g:set var="configEndpoint" value="${config.protocol.toLowerCase()}://${config.hosts.tokenize(',')[0]}:${config.port}"/></g:if>
    <tr class="s3ProxyField"><th>S3 Proxy Bucket Endpoint</th><td>
        <g:textField name="${prefix}.s3ProxyBucketEndpoint" value="${fieldValue(bean: bean, field: 's3ProxyBucketEndpoint') ?: configEndpoint}" size="60"/></td></tr>
    <tr class="s3ProxyField"><th>S3 Proxy Bucket Smart Client</th><td>
        <g:checkBox name="${prefix}.s3ProxyBucketSmartClient" value="${fieldValue(bean: bean, field: 's3ProxyBucketEndpoint') ? fieldValue(bean: bean, field: 's3ProxyBucketSmartClient') : config.smartClient}"/></td></tr>
    <tr class="s3ProxyField"><th>S3 Proxy Bucket Access Key</th><td>
        <g:textField name="${prefix}.s3ProxyBucketAccessKey" value="${fieldValue(bean: bean, field: 's3ProxyBucketAccessKey') ?: config.accessKey}"/></td></tr>
    <tr class="s3ProxyField"><th>S3 Proxy Bucket Secret Key</th><td>
        <g:passwordField name="${prefix}.s3ProxyBucketSecretKey" value="${fieldValue(bean: bean, field: 's3ProxyBucketSecretKey') ?: config.secretKey}" size="60"/>
        <span class="passwordToggle" data-target-id="${prefix}.s3ProxyBucketSecretKey">show</span></td></tr>
    <tr class="s3ProxyField"><th>S3 Proxy Bucket</th><td>
        <g:textField name="${prefix}.s3ProxyBucket" value="${fieldValue(bean: bean, field: 's3ProxyBucket') ?: config.configBucket}"/></td></tr>
    <tr class="s3ProxyField"><th>S3 Proxy Poll Interval (seconds)</th><td>
        <g:textField name="${prefix}.s3ProxyPollIntervalSecs" value="${fieldValue(bean: bean, field: 's3ProxyPollIntervalSecs')}"/>
        <span class="plugin-info glyphicon glyphicon-info-sign"
              data-content="The S3 Proxy polls the orchestration bucket at a specified interval (default 30 seconds).
                            This value should match that interval. When making any changes to the proxy configuration
                            (in the orchestration bucket), the migration will wait for all proxy instances to pick
                            up those changes."></span></td></tr>
    <tr class="s3ProxyField"><th>S3 Proxy Pre-Migration Bucket Wait Period (seconds)</th><td>
        <g:textField name="${prefix}.s3ProxyPreBucketMigrationWaitSecs" value="${fieldValue(bean: bean, field: 's3ProxyPreBucketMigrationWaitSecs')}"/>
        <span class="plugin-info glyphicon glyphicon-info-sign"
              data-content="After a bucket is moved into the migration state in the S3 Proxy, it's possible that
                            existing large writes to the Atmos may not be finished. This wait period gives those
                            large writes a chance to finish before the bucket migration begins."></span></td></tr>
</table>
<script>
    $(document).ready(function () {
        $("input[name='${prefix}.useS3Proxy']").change(function () {
            $(".s3ProxyField input").prop('disabled', !this.checked);
        });

        $("input[name='${prefix}.useS3Proxy']").change();

        $('#userMapUploadButton').hide().click(function (e) {
            e.preventDefault();
            var $file = $('#userMapFile');
            $file.prop('disabled', false);
            $file.trigger('click');
        })

        $('#userMapFile').change(function () {
            var $file = $('#userMapFile');

            // make sure we don't submit the file with the migration form
            $file.prop('disabled', true);

            // read file
            var file = $file[0].files[0];
            var reader = new FileReader();
            reader.readAsText(file, 'UTF-8');
            reader.onloadstart = function() { $('#spinner').fadeIn(); };
            reader.onloadend = function() { $('#spinner').fadeOut(); };
            reader.onload = function(e) {
                // file has been read
                var $target = $("#userTableBody");
                $target.html(''); // clear table

                var csv = e.target.result; // file contents

                // CSV line parser - assumes no quotes, commas, or line feeds in values
                var pattern = new RegExp(/^"?([^",\n]*)"?, *"?([^",\n]*)"?,? *"?([^",\n]*)"?.*$/gm);

                var selectedUsers = [${bean.userMap.keySet}];

                // loop through rows and add users to form
                var i = 0;
                var row = pattern.exec(csv);
                while (row) {
                    if (row[0].length === 0) continue; // skip blank lines
                    var atmosUid = row[1], ecsUser = row[2], ecsNamespace = row[3];
                    var $row = $($('#template-userMapRow').html());
                    $row.find('.atmosUid').text(atmosUid);
                    var $userCheckbox = $row.find('.userCheckbox input');
                    $userCheckbox.attr('id', '${prefix}.selectedUsers[' + i + ']');
                    $userCheckbox.attr('name', '${prefix}.selectedUsers[' + i + ']');
                    $userCheckbox.val(atmosUid);
                    $row.find('.ecsUser input').each(function () {
                        var $this = $(this);
                        $this.attr('id', '${prefix}.userMap[' + atmosUid + ']');
                        $this.attr('name', '${prefix}.userMap[' + atmosUid + ']');
                        $this.val(ecsUser);
                    });
                    $row.find('.ecsNamespace input').each(function () {
                        var $this = $(this);
                        $this.attr('id', '${prefix}.userNamespaceMap[' + atmosUid + ']');
                        $this.attr('name', '${prefix}.userNamespaceMap[' + atmosUid + ']');
                        $this.val(ecsNamespace);
                    });
                    $row.find('.testResult').attr('id', 'testResult[' + atmosUid + ']');
                    $target.append($row);
                    if ($.inArray(atmosUid, selectedUsers))
                        $row.find('.userCheckbox').prop("checked", true);
                    row = pattern.exec(csv);
                    i++;
                }
            };
        });
    });

    function setTargetNamespace() {
        event.preventDefault();
        if (confirm('Are you sure you want to set the target namespace for all mapped users?')) {
            var namespaceVal = $("#ecsNamespace").val();
            $('#userTableBody .ecsNamespace input').val(namespaceVal);
        }
    }

    function migrationTestComplete(json) {
        var $testResult = $('.testResult');
        $testResult.removeClass('alert-success').removeClass('alert-warning').removeClass('alert-danger');
        $('#atmosSubtenantTestResult').addClass('alert-' + json.atmosSubtenant.status).html(json.atmosSubtenant.message);
        $.each(json.users, function (key, value) {
            $('#testResult\\[' + key + '\\]').addClass('alert-' + value.status).html(value.message);
        });
        $('#s3ProxyTestResult').addClass('alert-' + json.s3Proxy.status).html(json.s3Proxy.message);
        $('#userMapTestResult').addClass('alert-' + json.userMapping.status).html(json.userMapping.message);
        $testResult.show().delay(10000).fadeOut();
    }
</script>

</body>
</html>

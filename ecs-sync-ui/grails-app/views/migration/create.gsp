<%@ page import="sync.ui.ScheduledSync; org.springframework.validation.FieldError" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>
<h2>Start ${migrationEntry.type.capitalize()} Migration</h2>

<g:if test="${flash.message}">
    <div class="alert alert-info" role="status">${flash.message}</div>
</g:if>
<g:if test="${migrationEntry.hasErrors() || migrationEntry.migrationConfig.hasErrors()}">
    <div class="alert alert-danger">
        <ul class="errors" role="alert">
            <g:eachError bean="${migrationEntry}" var="error">
                <li <g:if test="${error in FieldError}">data-field-id="${error.field}"</g:if>><g:message
                        error="${error}"/></li>
            </g:eachError>
            <g:eachError bean="${migrationEntry.migrationConfig}" var="error">
                <li <g:if test="${error in FieldError}">data-field-id="${error.field}"</g:if>><g:message
                        error="${error}"/></li>
            </g:eachError>
        </ul>
    </div>
</g:if>

<g:form name="migrationForm" action="start" method="POST" class="disable-enter">
    <g:hiddenField name="migrationEntry.type" value="${migrationEntry.type}" />
    <g:hiddenField name="migrationEntry.guid" value="${migrationEntry.guid}" />
    <g:hiddenField name="migrationEntry.migrationConfig.guid" value="${migrationEntry.migrationConfig.guid}" />
    <g:hiddenField name="migrationEntry.migrationConfig.sourceStorageType" value="${migrationEntry.migrationConfig.sourceStorageType}" />
    <g:hiddenField name="migrationEntry.migrationConfig.targetStorageType" value="${migrationEntry.migrationConfig.targetStorageType}" />

    <g:render template="/migration/${migrationEntry.type}"
              model="[bean: migrationEntry.migrationConfig, prefix: 'migrationEntry.migrationConfig']"/>

    <g:submitButton name="start" value="Start" class="btn btn-primary"/>
    <script>
        function migrationTestClick() {
            event.preventDefault();
            $.ajax({
                type: 'POST',
                url: '<g:createLink action="test"/>',
                data: $('#migrationForm').serialize(),
                dataType: 'json',
                success: function (data) {
                    // check if sub-form has a custom test response handler
                    if (typeof migrationTestComplete === 'function') {
                        migrationTestComplete(data);
                    } else {
                        $('#migration-test-results').text(data || 'Success!').show().delay(5000).fadeOut();
                    }
                },
                error: function(request, status, error) {
                    $('#migration-test-results').html(request.responseText).show().delay(10000).fadeOut();
                }
            });
        }
    </script>
    <button class="btn btn-primary" onclick="migrationTestClick()">Test</button>
    <div id="migration-test-results" class="alert alert-info" style="display: none;"></div>
</g:form>

</body>
</html>
<%@ page import="sync.ui.ScheduledSync; org.springframework.validation.FieldError" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta name="layout" content="main"/>
    <title>ECS Sync UI</title>
</head>

<body>
<h2>Edit ${storageEntry.type.capitalize()} Storage Cluster</h2>

<g:if test="${flash.message}">
    <div class="alert alert-info" role="status">${flash.message}</div>
</g:if>
<g:if test="${storageEntry.hasErrors() || storageEntry.storage.hasErrors()}">
    <div class="alert alert-danger">
        <ul class="errors" role="alert">
            <g:eachError bean="${storageEntry}" var="error">
                <li <g:if test="${error in FieldError}">data-field-id="${error.field}"</g:if>><g:message
                        error="${error}"/></li>
            </g:eachError>
            <g:eachError bean="${storageEntry.storage}" var="error">
                <li <g:if test="${error in FieldError}">data-field-id="${error.field}"</g:if>><g:message
                        error="${error}"/></li>
            </g:eachError>
        </ul>
    </div>
</g:if>

<g:form name="storageForm" action="update" method="PUT" class="disable-enter">
    <g:hiddenField name="storageEntry.type" value="${storageEntry.type}" />
    <g:hiddenField name="storageEntry.old_name" value="${storageEntry.name}" />
    <table class="table table-striped table-condensed kv-table">
        <tr><th>Name:</th><td><g:textField name="storageEntry.name" value="${storageEntry.name}"/></td></tr>
    </table>

    <g:render template="/storage/${storageEntry.type}"
              model="[bean: storageEntry.storage, prefix: 'storageEntry.storage']"/>

    <g:submitButton name="update" value="Update" class="btn btn-primary"/>
    <button class="btn btn-primary" onclick="event.preventDefault(); $.ajax('test', {
        type: 'POST', data: $('#storageForm').serialize(), success: function(data) {
            $('#storage-test-results').text(data || 'Success!').show().delay(5000).fadeOut();
        }
    })">Test</button>
    <div id="storage-test-results" class="alert alert-info" style="display: none;"></div>
</g:form>

</body>
</html>
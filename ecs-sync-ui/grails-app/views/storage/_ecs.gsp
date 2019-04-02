<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<body>

<g:if test="${flash.error}">
    <div class="alert alert-danger" role="alert">${flash.error}</div>
</g:if>

<table class="table table-striped table-condensed kv-table">
    <tr><th>Management Endpoint:</th><td><g:textField name="${prefix}.managementEndpoint" value="${bean.managementEndpoint}" size="60"/></td></tr>
    <tr><th>System Admin User:</th><td><g:textField name="${prefix}.sysAdminUser" value="${bean.sysAdminUser}"/></td></tr>
    <tr><th>System Admin Password:</th><td><g:passwordField name="${prefix}.sysAdminPassword" value="${bean.sysAdminPassword}"/></td></tr>
    <tr><th>S3 API Endpoint:</th><td><g:textField name="${prefix}.s3ApiEndpoint" value="${bean.s3ApiEndpoint}" size="60"/></td></tr>
    <tr><th>S3 Smart Client:</th><td><g:checkBox name="${prefix}.s3SmartClient" value="${bean.s3SmartClient}"/></td></tr>
</table>

</body>
</html>

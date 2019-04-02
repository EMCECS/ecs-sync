<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<body>

<g:if test="${flash.error}">
    <div class="alert alert-danger" role="alert">${flash.error}</div>
</g:if>

<table class="table table-striped table-condensed kv-table">
    <tr><th>Management Endpoint:</th><td><g:textField name="${prefix}.managementEndpoint" value="${bean.managementEndpoint}" size="60"/></td></tr>
    <tr><th>Tenant Name:</th><td><g:textField name="${prefix}.tenantName" value="${bean.tenantName}"/></td></tr>
    <tr><th>Tenant Admin User:</th><td><g:textField name="${prefix}.tenantAdminUser" value="${bean.tenantAdminUser}"/></td></tr>
    <tr><th>Tenant Admin Password:</th><td><g:passwordField name="${prefix}.tenantAdminPassword" value="${bean.tenantAdminPassword}"/></td></tr>
    <tr><th>Atmos API Endpoint:</th><td><g:textField name="${prefix}.atmosApiEndpoint" value="${bean.atmosApiEndpoint}" size="60"/></td></tr>
    <tr><th>S3 API Endpoint:</th><td><g:textField name="${prefix}.s3ApiEndpoint" value="${bean.s3ApiEndpoint}" size="60"/></td></tr>
</table>

</body>
</html>

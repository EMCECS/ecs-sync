%{--
  - Copyright (c) 2018 Dell Inc. or its subsidiaries. All Rights Reserved.
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

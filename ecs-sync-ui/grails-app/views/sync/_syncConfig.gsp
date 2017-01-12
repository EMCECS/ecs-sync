<%@ page import="org.springframework.validation.FieldError" contentType="text/html;charset=UTF-8" %>
<html>
<body>

<g:if test="${flash.error}">
    <div class="alert alert-danger" role="alert">${flash.error}</div>
</g:if>

<sync:syncConfig bean="${syncConfig}" prefix="${prefix}" />

</body>
</html>

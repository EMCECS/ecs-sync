<%@ page defaultCodec="none" %>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <title><g:layoutTitle default="Grails"/></title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <asset:stylesheet src="application.css"/>
    <asset:javascript src="application.js"/>

    <g:layoutHead/>
</head>
<body role="document">

    <nav class="navbar navbar-default navbar-fixed-top">
        <div class="container">
            <div class="navbar-header">
                <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
                    <span class="sr-only">Toggle navigation</span>
                    <span class="icon-bar"></span>
                    <span class="icon-bar"></span>
                    <span class="icon-bar"></span>
                </button>
                <a class="navbar-brand" href="#">ECS Sync UI</a>
            </div>

            <div id="navbar" class="navbar-collapse collapse">
                <ul class="nav navbar-nav navbar-left">
                    <li ${controllerName == 'status' ? 'class="active"' : ''}><a href="/status">Status</a></li>
                    <li ${controllerName == 'schedule' ? 'class="active"' : ''}><a href="/schedule">Schedule</a></li>
                    <li ${controllerName == 'report' ? 'class="active"' : ''}><a href="/report">Reports</a></li>
                    <li ${controllerName == 'uiConfig' ? 'class="active"' : ''}><a href="/uiConfig">Config</a></li>
                </ul>
            </div>
        </div>
    </nav>

    <div class="container" role="main">
        <g:layoutBody/>
    </div>

    <footer class="footer">
        <div class="container">
            <p class="text-muted">ECS Sync UI v<g:meta name="info.app.version" /></p>
        </div>
    </footer>

    <div id="spinner" class="spinner" style="display:none;"><g:message code="spinner.alt" default="Loading&hellip;"/></div>

</body>
</html>

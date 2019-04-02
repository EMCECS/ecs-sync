@echo off

set "INSTALL_DIR=C:\ecs-sync"
set "LIB_DIR=%INSTALL_DIR%\lib"
set "PATH_TO_JAR=%LIB_DIR%\ecs-sync-ctl.jar

set "JAVA_OPTS="

java %JAVA_OPTS% -jar "%PATH_TO_JAR%" %*
exit /b %ERRORLEVEL%

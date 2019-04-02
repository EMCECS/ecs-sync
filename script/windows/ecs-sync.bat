@echo off

set "INSTALL_DIR=C:\ecs-sync"
set "LIB_DIR=%INSTALL_DIR%\lib"
set "EXT_LIB_DIR=%LIB_DIR%\ext"
set "PATH_TO_JAR=%LIB_DIR%\ecs-sync.jar

set "JAVA_OPTS=-server -Xmx12G -XX:+UseParallelGC"

java %JAVA_OPTS% -cp "%PATH_TO_JAR%:%EXT_LIB_DIR%\*" com.emc.ecs.sync.EcsSync %*
exit /b %ERRORLEVEL%

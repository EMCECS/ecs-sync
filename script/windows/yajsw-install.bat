@ECHO OFF
SetLocal EnableDelayedExpansion
SET "INSTALL_DIR=C:\ecs-sync"
SET "YAJSW_DIR=%~1"
IF "%YAJSW_DIR%"=="" GOTO:YajswMissing
if NOT EXIST "%YAJSW_DIR%" GOTO:YajswMissing
GOTO:YajswFound

:YajswMissing
ECHO usage: %0 ^<YAJSW directory^>
ECHO note: YAJSW is required to install services
EXIT /B 1

:YajswFound
SET "SET_ENV_BAT=%~1\bat\setenv.bat"
SET "YAJSW_JAR=%~1\wrapper.jar"
IF NOT EXIST "%SET_ENV_BAT%" (
    ECHO could not find "%SET_ENV_BAT%"; make sure you provide the unzipped location of YAJSW
    EXIT /B 1
)
IF NOT EXIST "%YAJSW_JAR%" (
    ECHO could not find "%YAJSW_JAR%"; make sure you provide the unzipped location of YAJSW
    EXIT /B 1
)

SET "MAIN_SERVICE_DIR=%INSTALL_DIR%\service\ecs-sync"
SET "MAIN_CONF_FILE=%MAIN_SERVICE_DIR%\yajsw-ecs-sync.conf"
IF EXIST "%MAIN_CONF_FILE%" (
    ECHO found: %MAIN_CONF_FILE%
) ELSE (
    ECHO could not find %MAIN_CONF_FILE%; make sure batch file is in the same directory
    EXIT /B 1
)

SET "UI_SERVICE_DIR=%INSTALL_DIR%\service\ecs-sync-ui"
SET "UI_CONF_FILE=%UI_SERVICE_DIR%\yajsw-ecs-sync-ui.conf"
SET "UI_JAR=%INSTALL_DIR%\lib\ecs-sync-ui.jar"
IF EXIST "%UI_CONF_FILE%" (
    ECHO found: %UI_CONF_FILE%
) ELSE (
    ECHO could not find %MAIN_CONF_FILE%; make sure batch file is in the same directory
    EXIT /B 1
)

CALL "%YAJSW_DIR%\bat\setenv.bat"
ECHO installing main service...
CALL %wrapper_bat% -i "%MAIN_CONF_FILE%"
ECHO starting main service...
CALL %wrapper_bat% -t "%MAIN_CONF_FILE%"
IF EXIST "%UI_JAR%" (
    ECHO installing UI service...
    CALL !wrapper_bat! -i "%UI_CONF_FILE%"
    ECHO starting UI service...
    CALL !wrapper_bat! -t "%UI_CONF_FILE%"
) ELSE (
    ECHO UI jar was not found; not installing UI service
)

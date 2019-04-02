@ECHO OFF
SetLocal EnableDelayedExpansion
SET "INSTALL_DIR=C:\ecs-sync"

REM GOTO:TESTS

REM check for elevation
NET SESSION >NUL 2>&1
IF NOT %ERRORLEVEL% == 0 (
    ECHO please run this script in an elevated command prompt
    EXIT /B 1
)

CALL:ParentDir "%~dp0" DIST_DIR
CALL:MatchFile "%DIST_DIR%\ecs-sync-?.*.jar" MAIN_JAR
CALL:MatchFile "%DIST_DIR%\ecs-sync-ctl-?.*.jar" CTL_JAR
CALL:MatchFile "%DIST_DIR%\ecs-sync-ui-?.*.jar" UI_JAR
REM UI jar might be in parent directory (next to distribution zip)
IF "%UI_JAR%"=="" (
    CALL:ParentDir "%DIST_DIR%" PDIST_DIR
    CALL:MatchFile "!PDIST_DIR!\ecs-sync-ui-?.*.jar" UI_JAR
)

SET "BAT_DIR=%INSTALL_DIR%\bat"
SET "LIB_DIR=%INSTALL_DIR%\lib"
SET "EXT_LIB_DIR=%LIB_DIR%\ext"
SET "SERVICE_DIR=%INSTALL_DIR%\service"
SET "LOG_DIR=%INSTALL_DIR%\log"

ECHO DIST_DIR=%DIST_DIR%
ECHO MAIN_JAR=%MAIN_JAR%
ECHO UI_JAR=%UI_JAR%
ECHO INSTALL_DIR=%INSTALL_DIR%
ECHO BAT_DIR=%BAT_DIR%
ECHO LIB_DIR=%LIB_DIR%
ECHO EXT_LIB_DIR=%EXT_LIB_DIR%
ECHO SERVICE_DIR=%SERVICE_DIR%
ECHO LOG_DIR=%LOG_DIR%

IF NOT EXIST "%MAIN_JAR%" (
    ECHO Cannot find jar files. Please run this script from within the exploded distribution package
    EXIT /B 1
)

REM install dir
IF NOT EXIST "%INSTALL_DIR%" (
    ECHO creating %INSTALL_DIR%...
    MKDIR "%INSTALL_DIR%"
) ELSE (
    ECHO %INSTALL_DIR% folder already exists
)

REM bat dir
IF NOT EXIST "%BAT_DIR%" (
    ECHO creating %BAT_DIR%...
    MKDIR "%BAT_DIR%"
) ELSE (
    ECHO %BAT_DIR% already exists
)

REM lib dir
IF NOT EXIST "%LIB_DIR%" (
    ECHO creating %LIB_DIR%...
    MKDIR "%LIB_DIR%"
) ELSE (
    ECHO %LIB_DIR% already exists
)
IF NOT EXIST "%EXT_LIB_DIR%" (
    ECHO creating %EXT_LIB_DIR%...
    MKDIR "%EXT_LIB_DIR%"
) ELSE (
    ECHO %EXT_LIB_DIR% already exists
)

REM service dir
IF NOT EXIST "%SERVICE_DIR%" (
    ECHO creating %SERVICE_DIR%...
    MKDIR "%SERVICE_DIR%"
) ELSE (
    ECHO %SERVICE_DIR% already exists
)
SET "MAIN_SERVICE_DIR=%SERVICE_DIR%\ecs-sync"
IF NOT EXIST "%MAIN_SERVICE_DIR%" (
    ECHO creating %MAIN_SERVICE_DIR%...
    MKDIR "%MAIN_SERVICE_DIR%"
) ELSE (
    ECHO %MAIN_SERVICE_DIR% already exists
)
SET "UI_SERVICE_DIR=%SERVICE_DIR%\ecs-sync-ui"
IF NOT EXIST "%UI_SERVICE_DIR%" (
    ECHO creating %UI_SERVICE_DIR%...
    MKDIR "%UI_SERVICE_DIR%"
) ELSE (
    ECHO %UI_SERVICE_DIR% already exists
)

REM log dir
IF NOT EXIST "%LOG_DIR%" (
    ECHO creating %LOG_DIR%...
    MKDIR "%LOG_DIR%"
) ELSE (
    ECHO %LOG_DIR% already exists
)

REM batch files
ECHO installing batch files...
COPY /Y /A "%DIST_DIR%\windows\run.bat" "%BAT_DIR%"
COPY /Y /A "%DIST_DIR%\windows\ecs-sync.bat" "%BAT_DIR%"
COPY /Y /A "%DIST_DIR%\windows\ecs-sync-ctl.bat" "%BAT_DIR%"
COPY /Y /A "%DIST_DIR%\windows\ecs-sync-enc-passwd.bat" "%BAT_DIR%"
FOR /F %%R IN ('ECHO ";%PATH%;" ^| FIND /C /I ";%BAT_DIR%;"') DO SET "RESULT=%%R"
IF %RESULT% == 0 (
    ECHO updating PATH...
    SETX PATH "%PATH%;%BAT_DIR%"
) ELSE (
    ECHO PATH already contains %BAT_DIR%
)

REM main jar install
ECHO installing %MAIN_JAR%...
COPY /Y /B "%MAIN_JAR%" "%LIB_DIR%"
FOR %%J IN ("%MAIN_JAR%") DO SET "JAR_NAME=%%~nxJ"
PUSHD "%LIB_DIR%" & DEL /Q ecs-sync.jar 2> NUL & MKLINK ecs-sync.jar %JAR_NAME%
POPD

REM UI jar install
IF EXIST "%UI_JAR%" (
    ECHO installing %UI_JAR%...
    COPY /Y /B "%UI_JAR%" "%LIB_DIR%"
    FOR %%J IN ("%UI_JAR%") DO SET "JAR_NAME=%%~nxJ"
    PUSHD "%LIB_DIR%" & DEL /Q ecs-sync-ui.jar 2> NUL & MKLINK ecs-sync-ui.jar !JAR_NAME!
    POPD
) ELSE (
    ECHO UI jar is not present
)

REM ctl jar install
IF EXIST "%CTL_JAR%" (
    ECHO installing %CTL_JAR%...
    COPY /Y /B "%CTL_JAR%" "%LIB_DIR%"
    FOR %%J IN ("%CTL_JAR%") DO SET "JAR_NAME=%%~nxJ"
    PUSHD "%LIB_DIR%" & DEL /Q ecs-sync-ctl.jar 2> NUL & MKLINK ecs-sync-ctl.jar !JAR_NAME!
    POPD
) ELSE (
    ECHO ctl jar not available
)

REM service files
ECHO installing YAJSW service files...
COPY /Y /A "%DIST_DIR%\windows\yajsw-ecs-sync.conf" "%MAIN_SERVICE_DIR%"
COPY /Y /A "%DIST_DIR%\windows\yajsw-ecs-sync-ui.conf" "%UI_SERVICE_DIR%"

REM app config file
SET "CONFIG_FILE=application-production.yml"
IF NOT EXIST "%INSTALL_DIR%\%CONFIG_FILE%" (
    ECHO installing %CONFIG_FILE%...
    COPY /Y /A "%DIST_DIR%\windows\%CONFIG_FILE%" "%INSTALL_DIR%"
) ELSE (
    ECHO %CONFIG_FILE% already present
)

ECHO done!
ECHO.
ECHO please use yajsw-install.bat to install services

GOTO:EOF

REM ----------------
REM -- Function Definitions
REM ----------------

:MatchFile  -- returns the first file that matches the provided path expression
::          -- %~1: path expression; i.e. "c:\somedir\my-app-?.*.zip"
::          -- %~2: var name to store matching full file path; i.e. "ZIP_FILE"
    SetLocal
    CALL:ParentDir "%~1" D
    FOR /F "tokens=*" %%X IN ('DIR "%~1" /B 2^> NUL') DO SET "MATCH=%D%\%%~X"
  REM ECHO MatchFile(%~1): %MATCH%
    ( EndLocal
        SET "%~2=%MATCH%"
    )
GOTO:EOF

:ParentDir  -- gets parent dir of specified dir
::          -- %~1: dir/file for which to get parent
::          -- %~2: var name to store parent dir (excludes trailing slash)
    SetLocal
    SET "PD=%~dp1"
    SET "D=%~1"
    IF "%D:~-1%"=="\" IF NOT "%D:~-2,-1%"==":" (
        SET "D=!D:~0,-1!"
        FOR %%X IN ("!D!") DO SET "PD=%%~dpX"
    )
    IF NOT "%PD%"=="" IF NOT "%PD:~-2,-1%"==":" SET "PD=%PD:~0,-1%"
  REM ECHO ParentDir(%~1): %PD%
    ( EndLocal
        SET "%~2=%PD%"
    )
GOTO:EOF

REM ----------------
REM -- Test Cases (manually verified)
REM -- Note: uncomment echo statements in functions
REM ----------------
:TESTS

REM ParentDir tests
SET "TEST_DIR=C:\foo\bar\"
CALL:ParentDir "%TEST_DIR%" TEST_R
SET "TEST_DIR=C:\foo\bar"
CALL:ParentDir "%TEST_DIR%" TEST_R
SET "TEST_DIR=C:\foo\"
CALL:ParentDir "%TEST_DIR%" TEST_R
SET "TEST_DIR=C:\foo"
CALL:ParentDir "%TEST_DIR%" TEST_R
SET "TEST_DIR=C:\"
CALL:ParentDir "%TEST_DIR%" TEST_R
SET "TEST_DIR=C:\foo\bar\foo-?.*.jar"
CALL:ParentDir "%TEST_DIR%" TEST_R

REM MatchFile tests
SET "DIST_DIR=C:\Users\arnets\Downloads\ecs-sync-3.2.9"
SET "TEST_PATH=%DIST_DIR%\ecs-sync-?.*.jar"
CALL:MatchFile "%TEST_PATH%" TEST_R
SET "TEST_PATH=%DIST_DIR%\ecs-sync-ctl-?.*.jar"
CALL:MatchFile "%TEST_PATH%" TEST_R
SET "TEST_PATH=%DIST_DIR%\ecs-sync-ui-?.*.jar"
CALL:MatchFile "%TEST_PATH%" TEST_R
CALL:ParentDir "%DIST_DIR%" PDIST_DIR
CALL:MatchFile "%PDIST_DIR%\ecs-sync-ui-?.*.jar" TEST_R

GOTO:EOF

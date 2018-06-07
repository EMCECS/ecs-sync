@echo off
REM ################
REM # This is a sample batch file to run the EcsSync tool as a stand-alone
REM # instance to run a single job configured via XML
REM ################

REM specify any external jars here
REM set EXT_JARS=C:\some_other_dir\sqljdbc4.jar

set CLASSPATH=.;.\*
if "%EXT_JARS%" NEQ "" set CLASSPATH=%CLASSPATH%;%EXT_JARS%

if "%1%" == "" goto usage

java -classpath "%CLASSPATH%" com.emc.ecs.sync.EcsSync --xml-config "%1%"
exit /b %ERRORLEVEL%

:usage
echo usage:
echo     %0 ^<config-xml-file^>
exit /b 1

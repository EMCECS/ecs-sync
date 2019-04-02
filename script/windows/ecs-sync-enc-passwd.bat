@echo off

set "INSTALL_DIR=C:\ecs-sync"
set "LIB_DIR=%INSTALL_DIR%\lib"
set "PATH_TO_JAR=%LIB_DIR%\ecs-sync.jar

java -cp "%PATH_TO_JAR%" com.emc.ecs.sync.service.MySQLDbService encrypt-password

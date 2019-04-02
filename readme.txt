ecs-sync
=========

A utility for migrating unstructured data from one system to another.

readme.txt             - this file

NOTICE                 - license notice
LICENSE                - full license

ecs-sync-<ver>.jar     - ecs-sync application/service executable jar
ecs-sync-ctl-<ver>.jar - ecs-sync client interface executable jar

run.sh                 - scripts for executing stand-alone with an XML configuration file
run.bat

ova/                   - scripts and configuration for installing ecs-sync as a service or appliance/OVA on linux

windows/               - scripts and configuration for installing ecs-sync as a service on windows

mysql/                 - DB scripts. note: in the OVA, the ecssync DB user already exists, and ecs-sync will create
                       - status tables automatically

docker/                - scripts and configuration for running in docker containers (see docker/README)

sample/                - sample XML configuration files

doc/                   - javadocs

src/                   - source

building
=========

version is set in build.gradle

main distribution:
    ./gradlew :distZip

UI jar:
    cd ecs-sync-ui
    ./grailsw prod package
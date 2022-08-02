ecs-sync
=========

A utility for migrating unstructured data from one system to another.

readme.txt             - this file

NOTICE                 - license notice
LICENSE                - full license

ecs-sync-<ver>.jar     - ecs-sync application/service executable jar
ecs-sync-ctl-<ver>.jar - ecs-sync client interface executable jar

script/ova/            - scripts and configuration for installing ecs-sync as a service or appliance/OVA on linux

script/windows/        - scripts and configuration for installing ecs-sync as a service on windows

script/mysql/          - DB scripts. note: in the OVA, the ecssync DB user already exists, and ecs-sync will create
                       - status tables automatically

docker/                - scripts and configuration for running in docker containers (see docker/README)

sample/                - sample XML configuration files


building
=========

version is set in build.gradle

main distribution:
    ./gradlew :distZip [plugin-excludes]

UI jar:
    cd ecs-sync-ui
    ../gradlew -Dgrails.env=prod assemble [plugin-excludes]

plugin-excludes:
You can optionally exclude certain plugins from the build with the following parameters:
To exclude filters:
    -Pfilter.excludes="cas-extractors,cifs-ecs-ingester"
To exclude storage plugins:
    -Pstorage.excludes="cas,nfs"

testing
=========

copy test-resources/test.properties.template to $HOME,
remove the .template extension,
populate with appropriate system access/credentials

./gradlew testReport

#!/bin/sh
if [ ! -f "$1" ]; then
  echo usage:
  echo "    $0 <config-xml-file>"
  exit 1
fi

# parent directory
SYNC_DIR=$(dirname $(dirname $(readlink -f $0)))

if [ -z $(ls $SYNC_DIR/*.jar 2> /dev/null) ]; then
  echo "there are no jar files in $SYNC_DIR"
  exit 1
fi

docker run --rm --link running-haproxy:ecs-proxy --link running-mariadb:mysql -v ${SYNC_DIR}:/sync java:8 java --classpath /sync:/sync/* com.emc.vipr.sync.ViPRSync --spring-config "/sync/$1"

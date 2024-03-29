#!/bin/sh
#
# Copyright (c) 2015 Dell Inc. or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
if [ \( "$1" != "-m" -a ! -f "$1" \) -o \( "$1" = "-m" -a \( ! -f "$3" -o ! -d "$2" \) \) ]; then
  echo usage:
  echo "    $0 [-m <dir-to-mount>] <config-xml-file>"
  exit 1
fi

MY_DIR=$(dirname $(readlink -f $0))

# ecs-sync directory
SYNC_DIR=$(dirname "${MY_DIR}")

# check for mount option
if [ "$1" = "-m" ]; then
  MOUNT_DIR=$2
  # copy xml to sync dir
  cp "$3" "${SYNC_DIR}/_vs_docker.xml"
else
  # copy xml to sync dir
  cp "$1" "${SYNC_DIR}/_vs_docker.xml"
fi

# start haproxy container
if [ -f "${MY_DIR}/ecs_nodes" ]; then
  (cd "${MY_DIR}" && ./start-haproxy.sh)
  if [ $? -ne 0 ]; then
    echo "haproxy container failed"
    exit 1
  fi
fi

# start mariadb container
(cd "${MY_DIR}" && ./start-mariadb.sh)
if [ $? -ne 0 ]; then
  echo "mariadb container failed"
  exit 1
fi

if [ -z $(ls "${SYNC_DIR}"/*.jar 2> /dev/null) ]; then
  echo "there are no jar files in ${SYNC_DIR}"
  exit 1
fi

docker run --rm --link running-haproxy:ecs-proxy --link running-mariadb:mysql -v "${SYNC_DIR}":/sync ${MOUNT_DIR:+-v "$MOUNT_DIR:/data"} java:8 java -classpath /sync:/sync/* com.emc.ecs.sync.EcsSync --spring-config /sync/_vs_docker.xml

rm "${SYNC_DIR}/_vs_docker.xml"
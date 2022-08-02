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
docker ps -a | grep running-mariadb 2> /dev/null
if [ $? -ne 0 ]; then
  . $(dirname $0)/mysql.env
  cd mariadb
  sed -i '/ENV/,$d' Dockerfile
  sed -i "\$aENV MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD} MYSQL_USER=${MYSQL_USER} MYSQL_PASSWORD=${MYSQL_PASSWORD} MYSQL_DATABASE=${MYSQL_DATABASE}" Dockerfile
  docker build -t ecs-mariadb .
  docker run -d --name running-mariadb -p 3306:3306 ecs-mariadb
else
  docker start running-mariadb
fi

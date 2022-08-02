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
docker ps 2> /dev/null | grep running-haproxy
if [ $? -eq 0 ]; then
  echo you must stop the container first \(docker stop running-haproxy\)
  exit 1
fi
docker ps -a 2> /dev/null | grep running-haproxy
if [ $? -eq 0 ]; then
  docker rm running-haproxy
  docker rmi ecs-haproxy
  if [ -f haproxy/haproxy.cfg.inst ]; then
    rm haproxy/haproxy.cfg.inst
  fi
fi

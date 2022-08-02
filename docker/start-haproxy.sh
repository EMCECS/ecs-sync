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
docker ps -a | grep running-haproxy 2> /dev/null
if [ $? -ne 0 ]; then
  cp haproxy/haproxy.cfg haproxy/haproxy.cfg.inst
  awk '{X++;print "server ds0"X"-s3 "$1":9020 check"}' ecs_nodes >> haproxy/haproxy.cfg.inst
  docker build -t ecs-haproxy haproxy
  docker run -d --name running-haproxy ecs-haproxy
  rm haproxy/haproxy.cfg.inst
else
  docker start running-haproxy
fi

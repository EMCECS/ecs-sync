#!/bin/sh
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

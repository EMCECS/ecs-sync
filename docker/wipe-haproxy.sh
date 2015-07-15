#!/bin/sh
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

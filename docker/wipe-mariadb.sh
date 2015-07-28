#!/bin/sh
docker ps 2> /dev/null | grep running-mariadb
if [ $? -eq 0 ]; then
  echo you must stop the container first \(docker stop running-mariadb\)
  exit 1
fi
docker ps -a 2> /dev/null | grep running-mariadb
if [ $? -eq 0 ]; then
  docker rm running-mariadb
  docker rmi ecs-mariadb
fi

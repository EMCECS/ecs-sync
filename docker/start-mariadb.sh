#!/bin/sh
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

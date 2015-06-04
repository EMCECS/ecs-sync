#!/bin/sh
if [ -z "$1" ]
then
  echo "usage: $0 <new-user-password>"
  exit 1
fi

mysql -u root -p -e "set @password='$1';\. create_mysql_user_db.sql"

echo "url= jdbc:mysql://localhost:3306/vipr_sync?user=viprsync&amp;password=$1"
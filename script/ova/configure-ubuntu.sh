#!/bin/bash
#
# Copyright (c) 2016-2025 Dell Inc. or its subsidiaries. All Rights Reserved.
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
# Flags (1 = on, 0 = off)
# The OVA sets a hostname of ecssync.local by default - turn this off if you don't want that
SET_HOSTNAME=1
# This is required for the ecs-sync UI (installs and configures apache for SSL/auth, opens firewall ports)
# Turn this off if you will not use the UI
ENABLE_UI=1

if [ ! -f /etc/lsb-release ]; then
    echo "ERROR: This script is designed for Ubuntu-based systems"
    exit 1
fi
if [ "$(id -u)" != "0" ]; then
    echo "ERROR: Please run as root"
    exit 1
fi
DIST_DIR="$(cd "$(dirname $0)/.." && pwd)"
echo "DIST_DIR=${DIST_DIR}"

if ((SET_HOSTNAME)); then
    # set hostname
    hostname ecssync.local
fi
# refresh repos
apt update
# java
if ! java -version; then
    if ! apt install -y openjdk-8-jdk java-1.8.0-openjdk java-1.8.0-openjdk-devel; then
        echo 'ERROR: java installation failed: please install java 1.8+'
        exit 1
    fi
fi

# NFS/SMB client tools
apt install -y nfs-kernel-server samba-client cifs-utils

# analysis tools
apt install -y iperf telnet sysstat dnsutils unzip

# apache
if ((ENABLE_UI)); then
    # apache
    apt install -y apache2
    echo '==1=='
    # configure proxy and auth
    a2enmod headers
    a2enmod rewrite
    a2enmod http2
    a2enmod proxy
    a2enmod proxy_http
    a2enmod ssl

    #Enable SSL
    if [ ! -f /etc/apache2/ssl.key/ecs-sync.key ]; then
      echo "WARNING: /etc/apache2/ssl.key/ecs-sync.key does not exist. Generating self-signed certificate."
      if [ ! -f /etc/apache2/ssl.key ]; then
        mkdir -p /etc/apache2/ssl.key
      fi
      if [ ! -f /etc/apache2/ssl.crt ]; then
        mkdir -p /etc/apache2/ssl.crt
      fi
      openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /etc/apache2/ssl.key/ecs-sync.key -out /etc/apache2/ssl.crt/ecs-sync.crt
    fi

    echo '==2=='
    cp "${DIST_DIR}/ova/httpd/.htpasswd" /etc/apache2/
    cp "${DIST_DIR}/ova/apache2/conf.d/ecs-sync.conf" /etc/apache2/conf.d
    if [ -x "/usr/bin/systemctl" ]; then
        systemctl enable apache2
        echo '==3=='
        systemctl restart apache2
        echo '==4=='
    else
        chkconfig apache2 reset
        service apache2 restart
    fi
    if [ -x "/usr/bin/firewall-cmd" ]; then
        # allow apache to use the network
        getsebool httpd_can_network_connect
        if [ $? -eq 0 ]; then
            setsebool -P httpd_can_network_connect=1
        fi
        systemctl enable firewalld
        systemctl start firewalld
        # open ports in firewall
        firewall-cmd --permanent --add-port=80/tcp --add-port=443/tcp
        firewall-cmd --reload
    else
        echo 'NOTE: please verify that port 443 is open to incoming connections'
    fi
fi

# Install MariaDB or mySQL
echo "Do you want to proceed to install MariaDB?"
echo "y - Yes, proceed to install MariaDB."
echo "n - No, MariaDB or mySQL is already installed, skip this step."
echo "e - Exit, I will manually install mySQL."

while true; do
  read -p "Enter your choice (y/n/e): " yne
  case $yne in
    [Yy]* )
      apt install -y mariadb-server
      if [ $? -ne 0 ]; then
        echo 'ERROR: MariaDB installation failed: please retry... or install mySQL'
        exit 1
      fi
      break;;
    [Nn]* )
      echo "Skipping mySQL/MariaDB installation"
      break;;
    [Ee]* )
      echo "Please manually install mySQL or MariaDB"
      exit 1;;
    * ) echo "Please answer yes, no or exit";;
  esac
done

# enable UTF-8 support
if [ -d /etc/mysql.cnf.d ]; then
    cp "${DIST_DIR}/mysql/ecs-sync.cnf" /etc/mysql.cnf.d
else
    echo 'WARNING: could not find my.cnf.d directory - please manually configure features in mysql/ecs-sync.cnf'
fi
# enable mariadb/mysql service (not enabled by default)
MYSQL_SERVICE=mariadb.service
if [ -x "/usr/bin/systemctl" ]; then
    if [ ! -f "/usr/lib/systemd/system/${MYSQL_SERVICE}" ]; then MYSQL_SERVICE=mysql; fi
    systemctl daemon-reload
    systemctl enable ${MYSQL_SERVICE}
    systemctl start ${MYSQL_SERVICE}
else
    chkconfig ${MYSQL_SERVICE} reset
    service ${MYSQL_SERVICE} restart
fi

# remove test DBs and set root PW
echo '--- starting mysql_secure_installation ---'
mysql_secure_installation
echo '--- finished mysql_secure_installation ---'
# create database for ecs-sync
MYSQL_DIR="$(cd "$(dirname $0)/../mysql" && pwd)"
echo '--- creating mysql user ecssync ---'
echo 'Please enter the mySQL/mariaDB root password'
mysql -u root -p < "${MYSQL_DIR}/utf8/create_mysql_user_db.sql"
# sysctl tweaks (disable IPv6, limit swapping)
if ! grep -q vm.swappiness /etc/sysctl.conf; then echo '
net.ipv6.conf.all.disable_ipv6 = 1
net.ipv6.conf.default.disable_ipv6 = 1

vm.swappiness = 10' >> /etc/sysctl.conf
fi
# - need lots of network sockets and 10MB core dumps (in case of crashes)
echo '
*   soft    nofile  65535
*   hard    nofile  65535
*   soft    core    10485760
*   hard    core    10485760
' > /etc/security/limits.d/ecs-sync.conf
sysctl -p
# configure root's LD_LIBRARY_PATH for CAS SDK
if ! grep -q LD_LIBRARY_PATH ~root/.bash_profile; then echo '
export LD_LIBRARY_PATH=/usr/local/Centera_SDK/lib/64' >> ~root/.bash_profile
fi
# configure FP_LOG_STATE_PATH for CAS SDK
if ! grep -q FP_LOG_STATE_PATH ~root/.bash_profile; then echo '
export FP_LOG_STATE_PATH=/var/log/ecs-sync/cas-sdk.config' >> ~root/.bash_profile
fi
echo 'Done (please manually install iozone, bucket-perf and the CAS SDK)'
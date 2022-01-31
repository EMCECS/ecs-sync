#!/bin/bash
# Flags (1 = on, 0 = off)
# The OVA sets a hostname of ecssync.local by default - turn this off if you don't want that
SET_HOSTNAME=1
# This is required for the ecs-sync UI (installs and configures apache for SSL/auth, opens firewall ports)
# Turn this off if you will not use the UI
ENABLE_UI=1

if [ ! -f /etc/redhat-release ]; then
    echo "ERROR: This script is designed for RedHat-based systems"
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

# EPEL repo
if ! yum -y install epel-release; then
    echo 'NOTE: EPEL repository not configured.. you may have to manually install some packages'
fi

# java
if ! java -version; then
    if ! yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel; then
        echo "ERROR: java installation failed: please install java 1.8+"
        exit 1
    fi
fi

# NFS/SMB client tools
yum -y install nfs-utils nfs-utils-lib samba-client cifs-utils

# analysis tools
yum -y install iperf telnet sysstat bind-utils unzip

if ((ENABLE_UI)); then
    # apache
    yum -y install httpd mod_ssl
    # configure proxy and auth
    cp "${DIST_DIR}/ova/httpd/.htpasswd" /etc/httpd
    cp "${DIST_DIR}/ova/httpd/conf.d/ecs-sync.conf" /etc/httpd/conf.d
    if [ -x "/usr/bin/systemctl" ]; then
        systemctl enable httpd
        systemctl restart httpd
    else
        chkconfig httpd reset
        service httpd restart
    fi
    if [ -x "/bin/firewall-cmd" ]; then
        # allow apache to use the network
        setsebool -P httpd_can_network_connect=1
        # open ports in firewall
        firewall-cmd --permanent --add-port=80/tcp
        firewall-cmd --permanent --add-port=443/tcp
        systemctl restart firewalld
    else
        echo 'NOTE: please verify that port 443 is open to incoming connections'
    fi
fi

# mysql (mariadb)
if ! mysql -V; then
    if ! yum -y install mariadb-server; then
        echo 'ERROR: MariaDB install failed: please install MariaDB or mySQL v5.5 or newer'
        exit 1
    fi
fi
# enable UTF-8 support
if [ -d /etc/my.cnf.d ]; then
    cp "${DIST_DIR}/mysql/ecs-sync.cnf" /etc/my.cnf.d
else
    echo 'WARNING: could not find my.cnf.d directory - please manually configure features in mysql/ecs-sync.cnf'
fi
# enable mariadb/mysql service (not enabled by default)
MYSQL_SERVICE=mariadb.service
if [ -x "/usr/bin/systemctl" ]; then
    if [ ! -f "/usr/lib/systemd/system/${MYSQL_SERVICE}" ]; then MYSQL_SERVICE=mysql; fi
    mkdir -p /etc/systemd/system/${MYSQL_SERVICE}.d
    echo '[Service]
LimitNOFILE=65535
LimitNPROC=65535' > /etc/systemd/system/${MYSQL_SERVICE}.d/ecs-sync.conf
    systemctl daemon-reload
    systemctl enable ${MYSQL_SERVICE}
    systemctl start ${MYSQL_SERVICE}
else
    if [ ! -f "/etc/init.d/${MYSQL_SERVICE}" ]; then MYSQL_SERVICE=mysql; fi
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

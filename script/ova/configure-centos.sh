#!/bin/bash
if [ ! -f /etc/redhat-release ]; then
    echo "This script is designed for RedHat-based systems"
    exit 1
fi
if [ "$(id -u)" != "0" ]; then
    echo "Please run as root"
    exit 1
fi
DIST_DIR="$(cd "$(dirname $0)/.." && pwd)"
echo "DIST_DIR=${DIST_DIR}"

# set hostname
hostname ecssync.local

# EPEL repo
if ! yum -y install epel-release; then
    echo 'NOTE: EPEL repository not configured.. you may have to manually install some packages'
fi

# java
if [ ! -x "/usr/bin/java" ]; then
    if ! yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel; then
        echo "script failed: please install java 1.8"
        exit 1
    fi
fi

# NFS/SMB client tools
yum -y install nfs-utils nfs-utils-lib samba-client cifs-utils

# analysis tools
yum -y install iperf telnet sysstat bind-utils unzip

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

# mysql (mariadb)
if [ ! -x "/usr/bin/mysql" ]; then
    if ! yum -y install mariadb-server; then
        echo 'script failed: please install MariaDB or mySQL v5.5 or newer'
        exit 1
    fi
fi
# enable UTF-8 support
if [ -d /etc/my.cnf.d ]; then
    cp "${DIST_DIR}/mysql/ecs-sync.cnf" /etc/my.cnf.d
fi
MYSQL_SERVICE=mariadb.service
if [ -x "/usr/bin/systemctl" ]; then
    if [ ! -f "/usr/lib/systemd/system/${MYSQL_SERVICE}" ]; then MYSQL_SERVICE=mysql; fi
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

# sysctl tweaks
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

# configure LD_LIBRARY_PATH for CAS SDK
if ! grep -q LD_LIBRARY_PATH ~/.bash_profile; then echo '
export LD_LIBRARY_PATH=/usr/local/Centera_SDK/lib/64' >> ~/.bash_profile
fi
# configure FP_LOG_STATE_PATH for CAS SDK
if ! grep -q FP_LOG_STATE_PATH ~/.bash_profile; then echo '
export FP_LOG_STATE_PATH=/var/log/ecs-sync/cas-sdk.config' >> ~/.bash_profile
fi

echo 'Done (please manually install iozone, bucket-perf and the CAS SDK)'

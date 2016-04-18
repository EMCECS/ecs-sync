#!/bin/bash
if [ "$(id -u)" != "0" ]; then
   echo "Please run as root"
   exit 1
fi
shopt -s nullglob
OVA_DIR="$(cd "$(dirname $0)" && pwd)"
DIST_DIR="$(cd "${OVA_DIR}/.." && pwd)"
MAIN_JAR=$(echo "${DIST_DIR}"/ecs-sync-?.*.jar)
UI_JAR=$(echo "${DIST_DIR}"/ecs-sync-ui-?.*.jar)
USER="ecssync"
PASS="ECS-Sync"
INSTALL_DIR=/opt/emc/ecs-sync
LOG_DIR=/var/log/ecs-sync

echo "OVA_DIR=${OVA_DIR}"
echo "DIST_DIR=${DIST_DIR}"
echo "MAIN_JAR=${MAIN_JAR}"
echo "UI_JAR=${UI_JAR}"
echo "INSTALL_DIR=${INSTALL_DIR}"
echo "LOG_DIR=${LOG_DIR}"

if [ ! -f "${MAIN_JAR}" ]; then
    echo "Cannot find jar files. Please run this script from within the exploded distribution package"
    exit 1
fi

# user creation
id ${USER} 2>&1 > /dev/null
if [ $? -ne 0 ]; then
    echo "creating ${USER} user..."
    useradd -U -m ${USER}
    echo "${PASS}" | passwd --stdin ${USER}
else
    echo "${USER} user already exists"
fi

# install dir
if [ ! -d "${INSTALL_DIR}" ]; then
    echo "creating ${INSTALL_DIR}..."
    mkdir -p "${INSTALL_DIR}"
    chown ${USER}.${USER} "${INSTALL_DIR}"
else
    echo "${INSTALL_DIR} already exists"
fi

# main jar install and service
if [ -f "${MAIN_JAR}" ]; then
    echo "installing ${MAIN_JAR}..."
    cp "${MAIN_JAR}" "${INSTALL_DIR}"
    (cd "${INSTALL_DIR}" && rm -f ecs-sync.jar && ln -s "$(basename ${MAIN_JAR})" "ecs-sync.jar")
    echo "installing ecs-sync service..."
    cp "${OVA_DIR}/init.d/ecs-sync" /etc/init.d
    chkconfig --add ecs-sync && chkconfig ecs-sync reset
    service ecs-sync start
else
    echo "ERROR: cannot find ${MAIN_JAR}"
    exit 1
fi

# ui jar install and service
if [ -f "${UI_JAR}" ]; then
    echo "installing ${UI_JAR}..."
    cp "${UI_JAR}" "${INSTALL_DIR}"
    (cd "${INSTALL_DIR}" && rm -f ecs-sync-ui.jar && ln -s "$(basename ${UI_JAR})" "ecs-sync-ui.jar")
    echo "installing ecs-sync-ui service..."
    cp "${OVA_DIR}/init.d/ecs-sync-ui" /etc/init.d
    chkconfig --add ecs-sync-ui && chkconfig ecs-sync-ui reset
    service ecs-sync-ui start
else
    echo "${UI_JAR} not available"
fi

# config file
CONFIG_FILE=application-production.yml
if [ ! -f "${INSTALL_DIR}/${CONFIG_FILE}" ]; then
    echo "installing ${CONFIG_FILE}..."
    cp "${OVA_DIR}/${CONFIG_FILE}" "${INSTALL_DIR}"
else
    echo "${CONFIG_FILE} already present"
fi

# log dir
if [ ! -d "${LOG_DIR}" ]; then
    echo "creating ${LOG_DIR}..."
    mkdir -p "${LOG_DIR}"
    chown ${USER}.${USER} "${LOG_DIR}"
else
    echo "${LOG_DIR} already exists"
fi

# logrotate
LOGROTATE_D=/etc/logrotate.d
echo "configuring logrotate..."
cp "${OVA_DIR}/logrotate.d/ecs-sync" "${LOGROTATE_D}"

echo "done!"

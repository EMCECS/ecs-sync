#!/bin/bash
#
# Copyright (c) 2016-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
INSTALL_DIR=/opt/emc/ecs-sync
LOG_DIR=/var/log/ecs-sync

if [ "$(id -u)" != "0" ]; then
   echo "Please run as root"
   exit 1
fi
shopt -s nullglob
OVA_DIR="$(cd "$(dirname $0)" && pwd)"
DIST_DIR="$(cd "${OVA_DIR}/.." && pwd)"
MAIN_JAR=$(echo "${DIST_DIR}"/ecs-sync-?.*.jar)
UI_JAR=$(echo "${DIST_DIR}"/ecs-sync-ui-?.*.jar)
CTL_JAR=$(echo "${DIST_DIR}"/ecs-sync-ctl-?.*.jar)
USER="ecssync"
BIN_DIR="${INSTALL_DIR}/bin"
LIB_DIR="${INSTALL_DIR}/lib"
EXT_LIB_DIR="${LIB_DIR}/ext"

# UI jar might be in parent directory (next to distribution zip)
if [ ! -f "${UI_JAR}" ]; then
    PDIR="$(cd "${DIST_DIR}/.." && pwd)"
    UI_JAR=$(echo "${PDIR}"/ecs-sync-ui-?.*.jar)
fi

echo "OVA_DIR=${OVA_DIR}"
echo "DIST_DIR=${DIST_DIR}"
echo "MAIN_JAR=${MAIN_JAR}"
echo "UI_JAR=${UI_JAR}"
echo "INSTALL_DIR=${INSTALL_DIR}"
echo "LIB_DIR=${LIB_DIR}"
echo "EXT_LIB_DIR=${EXT_LIB_DIR}"
echo "LOG_DIR=${LOG_DIR}"

if [ ! -f "${MAIN_JAR}" ]; then
    echo "Cannot find jar files. Please run this script from within the exploded distribution package"
    exit 1
fi

# user creation
id ${USER} 2>&1 > /dev/null
if [ $? -ne 0 ]; then
    echo "creating service account ${USER}..."
    useradd -m -r -U ${USER}
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

# bin dir
if [ ! -d "${BIN_DIR}" ]; then
    echo "creating ${BIN_DIR}..."
    mkdir -p "${BIN_DIR}"
    cp -p "${OVA_DIR}"/bin/* "${BIN_DIR}"
    chown -R ${USER}.${USER} "${BIN_DIR}"
    # modify path
    export PATH="${BIN_DIR}:${PATH}"
    if ! grep -q "^PATH=" "$(eval echo "~${USER}")/.bash_profile"; then
        echo "export PATH=\"${BIN_DIR}:\$PATH\"" >> "$(eval echo "~${USER}")/.bash_profile"
    else
        if ! grep -q "${BIN_DIR}" $(eval echo "~${USER}")/.bash_profile; then
            sed -i "s~^\(PATH=\)\(.*\)$~\1${BIN_DIR}:\2~" $(eval echo "~${USER}")/.bash_profile
        fi
    fi
    if ! grep -q "${BIN_DIR}" ~root/.bash_profile; then
        sed -i "s~^\(PATH=\)\(.*\)$~\1${BIN_DIR}:\2~" ~root/.bash_profile
    fi
else
    echo "${BIN_DIR} already exists"
fi

# lib dir
if [ ! -d "${LIB_DIR}" ]; then
    echo "creating ${LIB_DIR}..."
    mkdir -p "${LIB_DIR}"
    chown ${USER}.${USER} "${LIB_DIR}"
else
    echo "${LIB_DIR} already exists"
fi
if [ ! -d "${EXT_LIB_DIR}" ]; then
    echo "creating ${EXT_LIB_DIR}..."
    mkdir -p "${EXT_LIB_DIR}"
    chown ${USER}.${USER} "${EXT_LIB_DIR}"
else
    echo "${EXT_LIB_DIR} already exists"
fi

# log dir
if [ ! -d "${LOG_DIR}" ]; then
    echo "creating ${LOG_DIR}..."
    mkdir -p "${LOG_DIR}"
    chown ${USER}.${USER} "${LOG_DIR}"
else
    echo "${LOG_DIR} already exists"
fi

# main jar install and service
if [ -f "${MAIN_JAR}" ]; then
    echo "installing ${MAIN_JAR}..."
    cp "${MAIN_JAR}" "${LIB_DIR}"
    chown ${USER}.${USER} "${LIB_DIR}/$(basename ${MAIN_JAR})"
    (cd "${LIB_DIR}" && rm -f ecs-sync.jar && ln -s "$(basename ${MAIN_JAR})" "ecs-sync.jar")
    echo "installing ecs-sync service..."
    if [ -d /run/systemd/system ]; then
      cp "${OVA_DIR}/bin/ecs-sync" "${BIN_DIR}"
      chmod +x "${BIN_DIR}/ecs-sync"
      cp "${OVA_DIR}/systemd/ecs-sync.service" /etc/systemd/system
    else
      cp "${OVA_DIR}/init.d/ecs-sync" /etc/init.d
      chmod 500 /etc/init.d/ecs-sync
    fi

    if [ -x "/usr/bin/systemctl" ]; then
        systemctl daemon-reload
        systemctl enable ecs-sync
        systemctl restart ecs-sync
    else
        chkconfig --add ecs-sync && chkconfig ecs-sync reset
        service ecs-sync restart
    fi
else
    echo "ERROR: cannot find ${MAIN_JAR}"
    exit 1
fi

# ui jar install and service
if [ -f "${UI_JAR}" ]; then
    echo "installing ${UI_JAR}..."
    cp "${UI_JAR}" "${LIB_DIR}"
    chown ${USER}.${USER} "${LIB_DIR}/$(basename ${UI_JAR})"
    (cd "${LIB_DIR}" && rm -f ecs-sync-ui.jar && ln -s "$(basename ${UI_JAR})" "ecs-sync-ui.jar")
    echo "installing ecs-sync-ui service..."
    if [ -d /run/systemd/system ]; then
      cp "${OVA_DIR}/bin/ecs-sync-ui" "${BIN_DIR}"
      chmod +x "${BIN_DIR}/ecs-sync-ui"
      cp "${OVA_DIR}/systemd/ecs-sync-ui.service" /etc/systemd/system
    else
      cp "${OVA_DIR}/init.d/ecs-sync-ui" /etc/init.d
      chmod 500 /etc/init.d/ecs-sync-ui
    fi

    if [ -x "/usr/bin/systemctl" ]; then
        systemctl daemon-reload
        systemctl enable ecs-sync-ui
        systemctl restart ecs-sync-ui
    else
        chkconfig --add ecs-sync-ui && chkconfig ecs-sync-ui reset
        service ecs-sync-ui restart
    fi
else
    echo "UI jar is not present"
fi

# ctl jar install
if [ -f "${CTL_JAR}" ]; then
    echo "installing ${CTL_JAR}..."
    cp "${CTL_JAR}" "${LIB_DIR}"
    chown ${USER}.${USER} "${LIB_DIR}/$(basename ${CTL_JAR})"
    (cd "${LIB_DIR}" && rm -f ecs-sync-ctl.jar && ln -s "$(basename ${CTL_JAR})" "ecs-sync-ctl.jar")
else
    echo "ctl jar not available"
fi

# config file
CONFIG_FILE=application-production.yml
if [ ! -f "${INSTALL_DIR}/${CONFIG_FILE}" ]; then
    echo "installing ${CONFIG_FILE}..."
    cp "${OVA_DIR}/${CONFIG_FILE}" "${INSTALL_DIR}"
else
    echo "${CONFIG_FILE} already present"
fi

# CAS SDK log config file
CAS_SDK_CONFIG_FILE=cas-sdk.config
if [ ! -f "${LOG_DIR}/${CAS_SDK_CONFIG_FILE}" ]; then
    echo "installing ${CAS_SDK_CONFIG_FILE}..."
    cp "${OVA_DIR}/${CAS_SDK_CONFIG_FILE}" "${LOG_DIR}"
else
    echo "${CAS_SDK_CONFIG_FILE} already present"
fi

# logrotate
LOGROTATE_D=/etc/logrotate.d
echo "configuring logrotate..."
cp "${OVA_DIR}/logrotate.d/ecs-sync" "${LOGROTATE_D}"

echo "done!"

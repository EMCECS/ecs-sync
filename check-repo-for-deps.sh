#!/usr/bin/env bash
#
# Copyright (c) 2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
########
# Script to check a given maven repository for all runtime dependencies required by modules in this project
########

REPO_URL=$1
if [ -z "${REPO_URL}" ]; then
  echo "usage: $0 <repo-base-URL> [--debug]"
  exit 1
fi
DEBUG=0
if [ "$2" == "--debug" ]; then
  echo "debug enabled"
  DEBUG=1
fi

# make sure URL ends with slash
if [[ ${REPO_URL} != */ ]]; then
  REPO_URL=${REPO_URL}/
fi

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

# locate project directories
for PROJ_DIR in $(find . -name build.gradle | grep -v '\./build.gradle' | grep -v buildSrc | xargs dirname); do
  pushd "${PROJ_DIR}" > /dev/null || exit 2
    echo "checking ${PROJ_DIR}..."

    # list maven dependency coordinates
    for MAVEN_DEP in $("${ROOT_DIR}"/gradlew dependencies --configuration runtimeClasspath \
        | grep -- "--- " \
        | grep -v "project " \
        | sed -e 's/:[^ :]* -> \([^ ]*\)/:\1/g' \
        | sed -e 's/^.*--- \([^ ]*\).*$/\1/g' \
        | sort -u); do
      ((DEBUG)) && echo "    -- dep: ${MAVEN_DEP}..."

      # translate maven coordinate to URL
      IFS=':' read -ra COORD <<< "${MAVEN_DEP}" # parse sections to an array
      MAVEN_GROUP=${COORD[0]}
      MAVEN_ARTIFACT=${COORD[1]}
      MAVEN_VERSION=${COORD[2]}
      # switch dots to slashes for the group when generating the link
      MAVEN_LINK=${REPO_URL}$(echo "${MAVEN_GROUP}" | sed -e 's/[.]/\//g')/${MAVEN_ARTIFACT}/${MAVEN_VERSION}
      ((DEBUG)) && echo "    -- link: ${MAVEN_LINK}"

      # check for presence in repository
      echo -n " ---- ${MAVEN_DEP} -> "
      if curl --output /dev/null --silent --head --fail "${MAVEN_LINK}" ; then
        echo "present"
      else
        echo "MISSING"
      fi
    done
  popd > /dev/null || exit 2
done
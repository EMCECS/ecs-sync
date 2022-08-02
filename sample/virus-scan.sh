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
# This script is intended as an example of how to scan objects for viruses.
# In this example, the LocalCacheFilter and ShellCommandFilter are used to copy objects to a local directory to be
# scanned before being sent to the target storage.  If the scan fails, the objects are considered infected and not
# copied to the target (this script can optionally copy them to a quarantine area)

# set this to the direction location configured in the LocalCacheFilter (localCacheRoot)
LOCAL_CACHE_ROOT=/tmp/sync-cache

# set to the quarantine root directory location
# blank this value to disable quarantine (the cache files will be deleted)
QUARANTINE_ROOT=/tmp/quarantine

# set to the user.group that will own quarantined files
QUARANTINE_OWNER=nobody.nogroup

# set to the virus scan command line (minus the file name)
SCAN_COMMAND="/usr/local/bin/savscan –eec"

# set to success exit codes
SUCCESS_EXIT_CODES=(0 20)

# check for file parameter
if [ -z "$1" ]; then
  echo "usage: $0 <file-to-scan>"
  echo "    path of file-to-scan is relative to the LocalCacheFilter's cache root directory"
  exit 1
fi

# resolve file path
echo "Object relative path: $1"
FILE_PATH=$(readlink -f "${LOCAL_CACHE_ROOT}/$1")
# safety check
if [[ ! "${FILE_PATH}" == "${LOCAL_CACHE_ROOT}/"* ]]; then
  echo "Possible malicious use: resolved file path ${FILE_PATH} is outside of local cache root ${LOCAL_CACHE_ROOT}"
  exit 1
fi
echo "Object absolute path: ${FILE_PATH}"

# execute scan on file
"${SCAN_COMMAND}" "${FILE_PATH}"
EXIT_CODE=$?

# check for success exit codes (defined above)
SUCCESS=0
for success_code in "${SUCCESS_EXIT_CODES[@]}"; do
  if ((EXIT_CODE == success_code)); then
    SUCCESS=1
  fi
done

if ((SUCCESS == 1)); then
  echo "Exit code ${EXIT_CODE} detected as success"
  exit 0
else
  >&2 echo "Exit code ${EXIT_CODE} detected as failure"

  # quarantine file
  if [ -n "${QUARANTINE_ROOT}" ]; then
    QUARANTINE_PATH=$(readlink -m "${QUARANTINE_ROOT}/$1")
    mkdir -p "$(dirname "${QUARANTINE_PATH}")"
    >&2 echo "Quarantining file ${FILE_PATH} to location ${QUARANTINE_PATH}"
    mv "${FILE_PATH}" "${QUARANTINE_PATH}"
    chown ${QUARANTINE_OWNER} "${QUARANTINE_PATH}"
    chmod 0400 "${QUARANTINE_PATH}"
  fi

  # make sure we don't exit with 0
  if ((EXIT_CODE == 0)); then exit 1
  else exit ${EXIT_CODE}
  fi
fi

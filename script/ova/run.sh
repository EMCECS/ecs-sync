#!/bin/sh
#
# Copyright (c) 2014-2018 Dell Inc. or its subsidiaries. All Rights Reserved.
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
################
# This is a sample shell script to run the EcsSync tool as a stand-alone
# instance to run a single job configured via XML
################

# specify any external jars here
#EXT_JARS=/some_other_dir/sqljdbc4.jar

CLASSPATH=.:./*
if [ -n "${EXT_JARS}" ]
then
  CLASSPATH=${CLASSPATH}:${EXT_JARS}
fi

if [ -z "$1" ]
then
  echo "This script is for running a stand-alone ecs-sync job outside of a service"
  echo "To submit a job to the running service, use ecs-sync-ctl"
  echo usage:
  echo "    $0 <config-xml-file>"
  exit 1
fi

java -classpath "${CLASSPATH}" com.emc.ecs.sync.EcsSync --xml-config "$1"
exit $?

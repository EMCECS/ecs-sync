#!/bin/sh
################
# This is a sample shell script to run the AtmosSync tool
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
  echo usage:
  echo "    $0 <config-xml-file>"
  exit 1
fi

java -classpath "${CLASSPATH}" com.emc.atmos.sync.AtmosSync2 --spring-config "$1"
exit $?

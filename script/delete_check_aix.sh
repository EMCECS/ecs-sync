#!/bin/sh

FUSER=`fuser "$1" 2>/dev/null | wc -w`

exit $FUSER

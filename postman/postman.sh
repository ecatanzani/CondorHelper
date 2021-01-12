#!/usr/bin/env bash

_NODE_NAME="sn-01.cr.cnaf.infn.it"
_PCMD="postman.py --config postman.conf"
_PY="python3"
_CMD="$_PY $_PCMD" 
_COMPLETED=0
_SLEEP_TIME=300

while [ $_COMPLETED -eq 0 ]
do
	NJOBS=`condor_q -name ${_NODE_NAME} ${USER} | grep "Total for query" | awk '{ print $4  }'`
	if [ $NJOBS -eq 0 ]
	then
		echo $_CMD
		$_CMD
		let _COMPLETED=1 
	fi
	sleep $_SLEEP_TIME
done
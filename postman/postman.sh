#!/usr/bin/env bash

_NODE_NAME="sn-01.cr.cnaf.infn.it"
_CMD="postman.py --config postman.conf"
_PY="python3"
_COMPLETED=0
_SLEEP_TIME=300

while [ $_COMPLETED -eq 0 ]
do
	NJOBS=`condor_q -name ${_NODE_NAME} ${USER} | grep "Total for query" | awk '{ print $4  }'`
	if [ $NJOBS -eq 0 ]
	then
		$PY $_CMD
		let _COMPLETED=1 
	fi
	sleep $_SLEEP_TIME
done
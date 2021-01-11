#!/usr/bin/env bash

_NODE_NAME="sn-01.cr.cnaf.infn.it"
_CMD="postman.py --config postman.conf"
_PY="python3"

NJOBS=`condor_q -name ${_NODE_NAME} ${USER} | grep "Total for query" | awk '{ print $4  }'`

if [ $NJOBS -eq 0 ]
then
	$PY $_CMD 
fi
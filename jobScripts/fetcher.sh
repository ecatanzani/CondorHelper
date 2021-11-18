#!/usr/bin/env bash

#_NODE_NAME="sn-01.cr.cnaf.infn.it"
_NODE_NAME="sn-02.cr.cnaf.infn.it"

source /usr/share/htc/condor/9/enable

function fetchData() { condor_transfer_data -name ${_NODE_NAME} $1; }
function removeJob() { condor_rm -name ${_NODE_NAME} $1; }

FIELDS=`condor_q -name ${_NODE_NAME} ${USER} | grep ID | grep -v OWNER | gawk 'NR==1{print NF}'`
if [ $FIELDS -eq 10 ]
then
	LIST=`condor_q -name ${_NODE_NAME} ${USER} | grep ID | grep -v OWNER | awk '{ if ($9 == "1" && $8 != "1" && $7 != 1) print $3 }'`
else
	LIST=`condor_q -name ${_NODE_NAME} ${USER} | grep ID | grep -v OWNER | awk '{ if ($9 == "1" && $8 != "1" && $7 != 1 && $6 != 1) print $3 }'`
fi

if [ -z "$LIST" ]
then
	echo "No Job to fetch..."
else
	for iJob in ${LIST}
	do 	
		fetchData $iJob
		removeJob $iJob
	done
fi
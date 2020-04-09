#!/usr/bin/env bash

function fetchData() { condor_transfer_data -name sn-01.cr.cnaf.infn.it $1; }
function removeJob() { condor_rm -name sn-01.cr.cnaf.infn.it $1; }

FIELDS=`condor_q -submitter ${USER} | grep ID | grep -v OWNER | gawk 'NR==1{print NF}'`
if [ $FIELDS -eq 10 ]
then
	LIST=`condor_q -submitter ${USER} | grep ID | grep -v OWNER | awk '{ if ($9 == "1" && $8 != "1" && $7 != 1) print $3 }'`
else
	LIST=`condor_q -submitter ${USER} | grep ID | grep -v OWNER | awk '{ if ($9 == "1" && $8 != "1" && $7 != 1 && $6 != 1) print $3 }'`
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
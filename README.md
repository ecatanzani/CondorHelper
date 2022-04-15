# CondorHelper
Job Submission Condor Helper

##

This is a Python interface to submit HTCondor jobs over INFN clusters.
The interface submits the requested number of jobs and automatically checks the results.

The output files are in **ROOT** format

Each single application has its own submission python script, which recals the helper library.

The **parser.sh** bash script in **jobScript** folder automatically parse the HTCondor output scheme and deletes completed jobs.

```
-- Schedd: sn-02.cr.cnaf.infn.it : <131.154.192.42:9618?... @ 04/15/22 10:27:26
OWNER      BATCH_NAME     SUBMITTED   DONE   RUN    IDLE  TOTAL JOB_IDS
ecatanzani ID: 2833183   4/15 09:44      _      1      _      1 2833183.0
ecatanzani ID: 2833184   4/15 09:44      _      1      _      1 2833184.0
ecatanzani ID: 2833185   4/15 09:44      _      1      _      1 2833185.0
ecatanzani ID: 2833186   4/15 09:44      _      1      _      1 2833186.0
ecatanzani ID: 2833187   4/15 09:44      _      1      _      1 2833187.0
ecatanzani ID: 2833188   4/15 09:44      _      1      _      1 2833188.0

```

##

The postman application sends mail notifications when all the jobs have been completed.
The mail server details can be set using the config file in postman folder.

```
##### Postman Mail Config Parameters #####

send_from  from_name
send_to  to_name
server  server_address
port port server_port
username  server_username
password  server_password
use_tls tls_status
```

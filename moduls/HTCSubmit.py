import subprocess


def submitJobs(opts, condorDirs):
    for folder in condorDirs:
        subFilePath = folder + str("/crawler.sub")
        subprocess.run(['condor_submit -name sn-01.cr.cnaf.infn.it -spool {}'.format(subFilePath)], shell=True, check=True)

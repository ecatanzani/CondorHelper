import subprocess


def submitJobs(opts, condorDirs):
    for folder in condorDirs:
        subprocess.run(
            ["condor_submit", "-name sn-01.cr.cnaf.infn.it -spool crawler.sub"], shell=True, check=True)

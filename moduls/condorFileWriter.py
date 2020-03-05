import os
import subprocess


def createCondorFiles(opts, condorDirs):
    for cDir in condorDirs:
        os.chdir(cDir)

        # Find out paths
        outputPath = os.getcwd() + "/" + "output.log"
        logPath = os.getcwd() + "/" + "output.clog"
        errPath = os.getcwd() + "/" + "output.err"

        # Writing sub file
        outSub = open("crawler.sub", 'w+')
        outSub.write("universe = vanilla\n")
        outSub.write("executable = script.sh\n")
        outSub.write('output = {}\n'.format(outputPath))
        outSub.write('error = {}\n'.format(errPath))
        outSub.write('log = {}\n'.format(logPath))
        outSub.write("ShouldTransferFiles = YES\n")
        outSub.write("WhenToTransferOutput = ON_EXIT\n")
        outSub.write("queue 1")
        outSub.close()
        if opts.verbose:
            print('HTCondor sub file created in: {}'.format(cDir))

        # Create python executable path
        pPath = os.getcwd()
        pPath, sep, tail = pPath.partition('/Assets')
        modulePath = pPath
        pPath += "/validate.py"

        # Create python executable args

        # Input data
        inputList = os.getcwd()
        inputList, sep, dirId = inputList.partition('/job_')
        inputList += "/dataList_" + dirId + ".txt"
        pyArgs = "-l " + inputList

        # Out ROOT file
        outputROOTfile = "outHisto_" + dirId + ".root"
        pyArgs += " -o "
        pyArgs += outputROOTfile

        # Verbosity
        pyArgs += " -v"

        # Validate moduls paths
        pyArgs += " -w "
        pyArgs += modulePath

        # Build executable bash script
        outScript = open("script.sh", "w+")
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
        outScript.write("dampe_init\n")
        outScript.write('python {} {}\n'.format(pPath, pyArgs))
        outScript.close()
        if opts.verbose:
            print('HTCondor bash script file created in: {}'.format(cDir))

        # Make bash script executable
        subprocess.run("chmod +x script.sh", shell=True, check=True)

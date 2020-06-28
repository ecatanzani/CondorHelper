import os
import subprocess
from tasks import dampe_task, MC_check_task


def createCondorFiles(opts, condorDirs, condorIdx):
    for idx, cDir in enumerate(condorDirs):

        # Find out paths
        outputPath = cDir + "/" + "output.log"
        logPath = cDir + "/" + "output.clog"
        errPath = cDir + "/" + "output.err"
        bashScriptPath = cDir + str("/script.sh")

        # Writing sub file
        subFilePath = cDir + str("/crawler.sub")
        try:
            with open(subFilePath, 'w') as outSub:
                outSub.write("universe = vanilla\n")
                outSub.write('executable = {}\n'.format(bashScriptPath))
                outSub.write('output = {}\n'.format(outputPath))
                outSub.write('error = {}\n'.format(errPath))
                outSub.write('log = {}\n'.format(logPath))
                outSub.write("ShouldTransferFiles = YES\n")
                outSub.write("WhenToTransferOutput = ON_EXIT\n")
                outSub.write("queue 1")
        except OSError:
            print('ERROR creating HTCondor sub file in: {}'.format(cDir))
            raise
        else:
            if opts.verbose:
                print('HTCondor sub file created in: {}'.format(cDir))

        # Build executable bash script
        dataListPath = cDir + str("/dataList_") + str(condorIdx[idx]) + ".txt"
        try:
            with open(bashScriptPath, "w") as outScript:
                if opts.task == "dmpExe":
                    dampe_task(opts, outScript, dataListPath, cDir)
                if opts.task == "MC_check":
                    MC_check_task(opts, outScript, dataListPath, cDir)
        except OSError:
            print('ERROR creating HTCondor bash script file in: {}'.format(cDir))
            raise
        else:
            if opts.verbose:
                print('HTCondor bash script file created in: {}'.format(cDir))

        # Make bash script executable
        subprocess.run('chmod +x {}'.format(bashScriptPath),
                       shell=True, check=True)

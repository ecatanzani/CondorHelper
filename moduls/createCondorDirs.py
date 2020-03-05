import os
from condorFileWriter import createCondorFiles


def createJobsDirs(opts, nDirs, outDir):

    if opts.verbose:
        print("Creating HTCondor output jobs dirs...")
    homeWD = os.getcwd()
    os.chdir(outDir)
    condorDirs = []
    for iDir in range(nDirs):
        tmpDirName = outDir + "/" + "job_" + str(iDir)
        if not os.path.isdir(tmpDirName):
            try:
                os.mkdir(tmpDirName)
            except OSError:
                print('Creation of the output directory {} failed'.format(tmpDirName))
                raise
            else:
                condorDirs.append(tmpDirName)
                if opts.verbose:
                    print('Succesfully created output directory: {}'.format(tmpDirName))
        else:
            condorDirs.append(tmpDirName)
            if opts.verbose:
                print('Using existing output path directory: {}'.format(tmpDirName))
    os.chdir(homeWD)

    # Create HTCondor sub file
    createCondorFiles(opts, condorDirs)

    return condorDirs

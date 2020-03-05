import os
import shutil

def writeListToFile(opts, dataList, outDir, listIdx, singleListOutput=None):
    from stuff import getListPath
    
    if singleListOutput is not None:
        listPath = singleListOutput
    homeWD = os.getcwd()
    os.chdir(outDir)
    tmpDirName = outDir + "/" + "job_" + str(listIdx)
    if not os.path.isdir(tmpDirName):
        try:
            os.mkdir(tmpDirName)
        except OSError:
            print('Creation of the output directory {} failed'.format(tmpDirName))
            raise
        else:
            if opts.verbose:
                print('Succesfully created output directory: {}'.format(tmpDirName))
    else:
        if opts.verbose:
            print('Using existing output path directory: {}'.format(tmpDirName))
        if opts.recreate:
            if opts.verbose:
                print("The directory, containing the following files, will be deleted...")
                print(os.listdir(tmpDirName))
            shutil.rmtree(tmpDirName)
            os.mkdir(tmpDirName)
    listPath = getListPath(tmpDirName, listIdx) + ".txt"
    try:
        with open(listPath, 'w') as outTmpList:
            for idx, file in enumerate(dataList):
                if idx == 0:
                    outTmpList.write(file)
                else:
                    outTmpList.write("\n%s" % file)
    except OSError:
        print('Creation of the output data list {} failed'.format(listPath))
        raise
    else:
        if opts.verbose:
            print('Created output list: {}'.format(listPath))
    os.chdir(homeWD)
    return tmpDirName

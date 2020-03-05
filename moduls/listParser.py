from writeList import writeListToFile


def parseInputList(opts):
    condorDirs = []
    condorIdx = []
    outDir = opts.directory
    inputList = open(opts.list, 'r')
    listIdx = 0
    dataList = []
    for fileName in inputList:
        if not fileName.startswith('.'):
            dataList.append(fileName.rstrip('\n'))
            if len(dataList) == opts.fileNumber:
                _dir_data = writeListToFile(opts, dataList, outDir, listIdx)
                if _dir_data[0]:
                    condorDirs.append(_dir_data[1])
                    condorIdx.append(listIdx)
                    listIdx += 1
                dataList.clear()
    if dataList:
        _dir_data = writeListToFile(opts, dataList, outDir, listIdx)
        if _dir_data[0]:
            condorDirs.append(_dir_data[1])
            condorIdx.append(listIdx)
            listIdx += 1
        dataList.clear()
    return listIdx, condorDirs, condorIdx

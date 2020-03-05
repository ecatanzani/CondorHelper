from writeList import writeListToFile


def parseInputList(opts):
    condorDirs = []
    outDir = opts.directory
    inputList = open(opts.list, 'r')
    listIdx = 0
    dataList = []
    for idx, fileName in enumerate(inputList):
        if not fileName.startswith('.'):
            dataList.append(fileName.rstrip('\n'))
            if len(dataList) == opts.fileNumber:
                condorDirs.append(writeListToFile(opts, dataList, outDir, listIdx))
                dataList.clear()
                listIdx += 1
    if dataList:
        condorDirs.append(writeListToFile(opts, dataList, outDir, listIdx))
        dataList.clear()
        listIdx += 1
    return listIdx, condorDirs

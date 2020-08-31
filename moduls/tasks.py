
def eFlux_acceptance_task(opts, outScript, dataListPath, cDir):
    tmpOutDir = cDir + str("/outFiles")
    outScript.write("#!/usr/bin/env bash\n")
    outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
    outScript.write("dampe_init trunk\n")
    outScript.write('mkdir {}\n'.format(tmpOutDir))
    outScript.write(
        '{} -a {} -d {} -w {} -v'.format(opts.executable, dataListPath, tmpOutDir, opts.config))
    
def eFlux_task(opts, outScript, dataListPath, cDir):
    tmpOutDir = cDir + str("/outFiles")
    outScript.write("#!/usr/bin/env bash\n")
    outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
    outScript.write("dampe_init trunk\n")
    outScript.write('mkdir {}\n'.format(tmpOutDir))
    outScript.write(
        '{} -i {} -d {} -w {} -g {} -b {} -t {} -v'.format(opts.executable, dataListPath, tmpOutDir, opts.config, opts.geometry, opts.background, opts.livetime))


def MC_check_task(opts, outScript, dataListPath, cDir):
    tmpOutDir = cDir + str("/outFiles")
    outScript.write("#!/usr/bin/env bash\n")
    outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
    outScript.write("dampe_init trunk\n")
    outScript.write('mkdir {}\n'.format(tmpOutDir))
    outScript.write(
        'python {} -l {} -d {} -v'.format(opts.executable, dataListPath, tmpOutDir))

def STKcharge_task(opts, outScript, dataListPath, cDir):
    tmpOutDir = cDir + str("/outFiles")
    outScript.write("#!/usr/bin/env bash\n")
    outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
    outScript.write("dampe_init trunk\n")
    outScript.write('mkdir {}\n'.format(tmpOutDir))
    outScript.write(
        '{} -i {} -d {} -v'.format(opts.executable, dataListPath, tmpOutDir))
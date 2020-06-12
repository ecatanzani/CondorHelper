
def eFlux_task(opts, completeSoftware, outScript, dataListPath, cDir):
    tmpOutDir = cDir + str("/outFiles")
    if completeSoftware:
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
        outScript.write("dampe_init\n")
        outScript.write('mkdir {}\n'.format(tmpOutDir))
        outScript.write(
            '{} -a {} -d {} -v'.format(opts.executable, dataListPath, tmpOutDir))
    else:
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write(
            "source /cvmfs/sft.cern.ch/lcg/app/releases/ROOT/5.34.36/x86_64-centos7-gcc48-opt/root/bin/thisroot.sh\n")
        outScript.write(
            'cd /storage/gpfs_data/dampe/users/ecatanzani/Softwares/DAMPE/Event && source thisdmpeventclass.sh && cd {}\n'.format(cDir))
        outScript.write('mkdir {}\n'.format(tmpOutDir))
        outScript.write(
            '{} -a {} -d {} -v'.format(opts.executable, dataListPath, tmpOutDir))


def MC_check_task(opts, outScript, dataListPath, cDir):
    tmpOutDir = cDir + str("/outFiles")
    outScript.write("#!/usr/bin/env bash\n")
    outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
    outScript.write("dampe_init\n")
    outScript.write('mkdir {}\n'.format(tmpOutDir))
    outScript.write(
        'python {} -l {} -d {} -v'.format(opts.executable, dataListPath, tmpOutDir))

#!/usr/bin/env python3
import os
import sys
from argparse import ArgumentParser


def main(args=None):
    # Parsing options
    parser = ArgumentParser(
        usage="Usage: %(prog)s [options]", description="Condor Deploy Helper")

    parser.add_argument("-l", "--list", type=str, dest='list', help='Input file list')
    parser.add_argument("-d", "--dir", type=str, dest='directory', help='Target Directory')
    parser.add_argument("-i", "--iterate", type=str, dest='iterate', help='Iterate through Target Directory - nTuples analysis')
    parser.add_argument("-x", "--executable", type=str, dest='executable', help='Analysis script')
    parser.add_argument("-g", "--geometry", type=str, dest='geometry', help='MC electron acceptance file')
    parser.add_argument("-b", "--background", type=str, dest='background', help='MC proton background file')
    parser.add_argument("-e", "--livetime", type=str, dest='livetime', help='DAMPE acqusition livetime')
    parser.add_argument("-c", "--config", type=str, dest='config', help='Software Config Directory')
    parser.add_argument("-n", "--number", type=int, dest='fileNumber', const=100, nargs='?', help='number of files per job')
    parser.add_argument("-v", "--verbose", dest='verbose', default=False, action='store_true', help='run in high verbosity mode')
    parser.add_argument("-r", "--recreate", dest='recreate', default=False, action='store_true', help='recreate output dirs if present')
    parser.add_argument("-t", "--task", type=str, dest='task', help='Define job task')

    opts = parser.parse_args(args)

    # Load parsing functions
    sys.path.append("moduls")
    from listParser import parseInputList
    from HTCSubmit import submitJobs
    from condorFileWriter import createCondorFiles

    if opts.list:
        nDirs, condorDirs, condorIdx = parseInputList(opts)
        # Create Condor files
        createCondorFiles(opts, condorDirs, condorIdx)
        # Submit condor jobs
        submitJobs(opts, condorDirs)
    if opts.iterate and (not opts.list) and (not opts.directory):
        nTuples_wdcontent = [ opts.iterate + "/" + tmp_dir for tmp_dir in os.listdir(opts.iterate) ]
        for dir in nTuples_wdcontent:
            if "201" in dir:
                opts.directory = dir
                opts.list = dir + "/dataFileList.txt"
                nDirs, condorDirs, condorIdx = parseInputList(opts)
                # Create Condor files
                createCondorFiles(opts, condorDirs, condorIdx)
                # Submit condor jobs
                submitJobs(opts, condorDirs)

    

    


if __name__ == '__main__':
    main()

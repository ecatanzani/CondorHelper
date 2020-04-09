#!/usr/bin/env python3
import sys
from argparse import ArgumentParser


def main(args=None):
    # Parsing options
    parser = ArgumentParser(
        usage="Usage: %(prog)s [options]", description="Condor Deploy Helper")
    
    parser.add_argument("-l", "--list", type=str,
                        dest='list', help='Input file list')
    parser.add_argument("-d", "--dir", type=str,
                        dest='directory', help='Target Directory')
    parser.add_argument("-x", "--executable", type=str,
                        dest='executable', help='Analysis script')                    
    parser.add_argument("-n", "--number", type=int, dest='fileNumber',
                        const=100, nargs='?', help='number of files per job')
    parser.add_argument("-i", "--installation", type=str,
                        dest='installation', help='Load local DAMPE installation')                    
    parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                        action='store_true', help='run in high verbosity mode')
    parser.add_argument("-r", "--recreate", dest='recreate', default=False,
                        action='store_true', help='recreate output dirs if present')

    opts = parser.parse_args(args)

    # Load parsing functions
    sys.path.append("moduls")
    from listParser import parseInputList
    from HTCSubmit import submitJobs
    from condorFileWriter import createCondorFiles
    
    if opts.list:
        nDirs, condorDirs, condorIdx = parseInputList(opts)
    
    # Create Condor files
    if opts.installation:
        createCondorFiles(opts, condorDirs, condorIdx, False)
    else:
        createCondorFiles(opts, condorDirs, condorIdx)

    # Submit condor jobs
    submitJobs(opts, condorDirs)


if __name__ == '__main__':
    main()

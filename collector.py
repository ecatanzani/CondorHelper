import os
import helper
from argparse import ArgumentParser

def get_folder_index(output: str) -> int:
    jobs_folder = [file for file in os.listdir(output) if file.startswith('job_')]
    return max([int(file[file.rfind('_')+1:]) for file in jobs_folder])+1

def main(args=None):
    parser = ArgumentParser(description='DAMPE all-electron collector facility')
    parser.add_argument("-l", "--list", type=str,
                        dest='list', help='Input DATA/MC list')
    parser.add_argument("-c", "--config", type=str,
                        dest='config', help='Software Config Directory')
    parser.add_argument("-o", "--output", type=str,
                        dest='output', help='HTC output directory')
    parser.add_argument("-m", "--mc", dest='mc',
                        default=False, action='store_true', help='MC event collector')
    parser.add_argument("-f", "--file", type=int, dest='file',
                        const=100, nargs='?', help='files to process in job')
    parser.add_argument("-x", "--executable", type=str,
                        dest='executable', help='Analysis script')

    parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                        action='store_true', help='run in high verbosity mode')
    parser.add_argument("-r", "--recreate", dest='recreate', default=False,
                        action='store_true', help='recreate output dirs if present')
    parser.add_argument("-a", "--append", dest='append', default=False,
                        action='store_true', help='append jobs folder to existing directory')
    
    opts = parser.parse_args(args)
    collector_helper = helper.helper()
    
    pars = {
        "list": opts.list,
        "config": opts.config,
        "output": opts.output,
        "mc": opts.mc,
        "files": opts.file,
        "executable": opts.executable,
        "verbose": opts.verbose,
        "recreate": opts.recreate
    }

    task = {
        "collector": True, 
        "kompressor": False, 
        "aladin": False, 
        "split": False, 
        "acceptance": False, 
        "efficiency": False,
        "signal_selection": False,
        "xtrl": False,
        "selection_split": False
    }

    start_idx = get_folder_index(opts.output) if opts.append else 0
        
    collector_helper.parse_input_list(pars, start_idx)
    collector_helper.create_condor_files(pars, task)
    collector_helper.submit_jobs()

if __name__ == '__main__':
    main()
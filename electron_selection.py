import helper
import argparse

import helper
import argparse

def main(args=None):
    parser = argparse.ArgumentParser(description='DAMPE electron-selection facility')
    parser.add_argument("-l", "--list", type=str,
                        dest='list', help='Input MC list')
    parser.add_argument("-c", "--config", type=str,
                        dest='config', help='Collector Config Directory')
    parser.add_argument("-o", "--output", type=str,
                        dest='output', help='HTC output directory')
    parser.add_argument("-m", "--learning_method", type=str,
                        dest='config', help='TMVA learning method')
    parser.add_argument("-f", "--file", type=int, dest='file',
                        const=10, nargs='?', help='files to process in job')
    parser.add_argument("-x", "--executable", type=str,
                        dest='executable', help='Analysis script')
    parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                        action='store_true', help='run in high verbosity mode')
    parser.add_argument("-n", "--new", dest='new', default=False,
                        action='store_true', help='recreate output dirs if present')

    opts = parser.parse_args(args)
    signal_selection_helper = helper.helper()

    pars = {
        "list": opts.list,
        "output": opts.output,
        "config": opts.config,
        "lm": opts.lm,
        "files": opts.file,
        "executable": opts.executable,
        "verbose": opts.verbose,
        "recreate": opts.new
    }

    task = {
        "collector": False, 
        "kompressor": False, 
        "aladin": False, 
        "split": False, 
        "acceptance": False, 
        "efficiency": False,
        "signal_selection": True
    }

    signal_selection_helper.parse_input_list(pars, start_idx=0)
    signal_selection_helper.create_condor_files(pars, task)
    signal_selection_helper.submit_jobs()

if __name__ == '__main__':
    main()
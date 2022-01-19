import helper
import argparse

def main(args=None):
    parser = argparse.ArgumentParser(description='DAMPE Efficiency facility')
    parser.add_argument("-l", "--list", type=str,
                        dest='list', help='Input MC list')
    parser.add_argument("-c", "--config", type=str,
                        dest='config', help='Collector Config Directory')
    parser.add_argument("-o", "--output", type=str,
                        dest='output', help='HTC output directory')
    parser.add_argument("-m", "--mc", dest='mc',
                        default=False, action='store_true', help='MC event collector')
    parser.add_argument("-f", "--file", type=int, dest='file',
                        const=10, nargs='?', help='files to process in job')
    parser.add_argument("-x", "--executable", type=str,
                        dest='executable', help='Analysis script')
    parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                        action='store_true', help='run in high verbosity mode')
    parser.add_argument("-n", "--new", dest='new', default=False,
                        action='store_true', help='recreate output dirs if present')

    opts = parser.parse_args(args)
    efficiency_helper = helper.helper()

    pars = {
        "list": opts.list,
        "output": opts.output,
        "config": opts.config,
        "mc": opts.mc,
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
        "efficiency": True,
        "signal_selection": False,
        "xtrl": False,
        "selection_split": False,
        "bdt_electron_selection": False
    }

    efficiency_helper.parse_input_list(pars, start_idx=0)
    efficiency_helper.create_condor_files(pars, task)
    efficiency_helper.submit_jobs()

if __name__ == '__main__':
    main()
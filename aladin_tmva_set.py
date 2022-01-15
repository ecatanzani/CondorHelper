import helper
import argparse


def main(args=None):
    parser = argparse.ArgumentParser(description='DAMPE Aladin TMVA facility')
    parser.add_argument("-l", "--list", type=str,
                        dest='list', help='Input DATA/MC list')
    parser.add_argument("-c", "--config", type=str,
                        dest='config', help='Local TMVA config directory')
    parser.add_argument("-o", "--output", type=str,
                        dest='output', help='HTC output directory')
    parser.add_argument("-s", "--split", dest='split', default=False,
                        action='store_true', help='Split data-set in Test/Train')
    parser.add_argument("-f", "--file", type=int, dest='file',
                        const=10, nargs='?', help='files to process in job')
    parser.add_argument("-x", "--executable", type=str,
                        dest='executable', help='Analysis script')
    parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                        action='store_true', help='run in high verbosity mode')
    parser.add_argument("-n", "--new", dest='new', default=False,
                        action='store_true', help='recreate output dirs if present')
    
    opts = parser.parse_args(args)
    aladin_tmva_helper = helper.helper()

    pars = {
        "list": opts.list,
        "config": opts.config,
        "output": opts.output,
        "split": opts.split,
        "aladin_tmva": True,
        "files": opts.file,
        "executable": opts.executable,
        "verbose": opts.verbose,
        "recreate": opts.new
    }

    task = {
        "collector": False, 
        "kompressor": False, 
        "aladin": True, 
        "split": False, 
        "acceptance": False, 
        "efficiency": False,
        "signal_selection": False,
        "xtrl": False
    }

    aladin_tmva_helper.parse_input_list(pars, start_idx=0)
    aladin_tmva_helper.create_condor_files(pars, task)
    aladin_tmva_helper.submit_jobs()

if __name__ == '__main__':
    main()
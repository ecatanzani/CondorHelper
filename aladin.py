import helper
import argparse

def set_files(opts: argparse.ArgumentParser) -> tuple:
    _recursive = False
    if opts.likelihood or opts.fit:
        opts.file = 1
        _recursive = True
    return (_recursive, opts.file)

def main(args=None):
    parser = argparse.ArgumentParser(description='DAMPE Aladin facility')
    parser.add_argument("-l", "--list", type=str,
                        dest='list', help='Input DATA/MC list')
    parser.add_argument("-c", "--config", type=str,
                        dest='config', help='Collector config directory')
    parser.add_argument("-o", "--output", type=str,
                        dest='output', help='HTC output directory')
    parser.add_argument("-m", "--mc", dest='mc',
                        default=False, action='store_true', help='MC event collector')
    parser.add_argument("-r", "--regularize", type=str,
                        dest='regularize', help='/path/to/regularized/tree - BDT variables regularizer facility')
    parser.add_argument("-g", "--gaussianize", dest='gaussianize', default=False,
                        action='store_true', help='BDT variables gaussianizer facility')
    parser.add_argument("-k", "--likelihood", dest='likelihood', default=False,
                        action='store_true', help='likelihood analysis facility')
    parser.add_argument("-t", "--fit", dest='fit', default=False,
                        action='store_true', help='fit analysis facility')
    parser.add_argument("-e", "--export_vars", type=str,
                        dest='export_vars', help='/path/to/labda/tree - TMVA variables facility')
    parser.add_argument("-f", "--file", type=int, dest='file',
                        const=10, nargs='?', help='files to process in job')
    parser.add_argument("-x", "--executable", type=str,
                        dest='executable', help='Analysis script')
    parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                        action='store_true', help='run in high verbosity mode')
    parser.add_argument("-n", "--new", dest='new', default=False,
                        action='store_true', help='recreate output dirs if present')
    
    opts = parser.parse_args(args)
    aladin_helper = helper.helper()

    (recursive, opts.file) = set_files(opts)

    pars = {
        "list": opts.list,
        "config": opts.config,
        "output": opts.output,
        "mc": opts.mc,
        "regularize": opts.regularize,
        "gaussianize": opts.gaussianize,
        "likelihood": opts.likelihood,
        "fit": opts.fit,
        "tmva": opts.export_vars,
        "aladin_tmva": False,
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
        "xtrl": False,
        "selection_split": False,
        "bdt_electron_selection": False
    }

    aladin_helper.parse_input_list(pars, start_idx=0, recursive=recursive)
    aladin_helper.create_condor_files(pars, task)
    aladin_helper.submit_jobs()

if __name__ == '__main__':
    main()
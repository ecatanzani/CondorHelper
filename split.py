import helper
import argparse

def main(args=None):
    parser = argparse.ArgumentParser(description='Aladin Split facility')

    parser.add_argument("-i", "--input", type=str,
                        dest='input', help='Input condor jobs WD')
    parser.add_argument("-c", "--config", type=str,
                            dest='config', help='Collector Config Directory')
    parser.add_argument("-o", "--output", type=str,
                            dest='output', help='output ROOT file')
    parser.add_argument("-m", "--mc", dest='mc',
                            default=False, action='store_true', help='MC event collector')
    parser.add_argument("-f", "--file", type=int, dest='file',
                            const=10, nargs='?', help='files to process in job')
    parser.add_argument("-x", "--executable", type=str,
                        dest='executable', help='Analysis script')
    parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                        action='store_true', help='run in high verbosity mode')
    parser.add_argument("-r", "--recreate", dest='recreate', default=False,
                        action='store_true', help='recreate output dirs if present')

    opts = parser.parse_args(args)
    aladinsplit_helper = helper.helper()

    pars = {
        "input": opts.input,
        "output": opts.output,
        "config": opts.config,
        "mc": opts.mc,
        "files": opts.file,
        "executable": opts.executable,
        "verbose": opts.verbose,
        "recreate": opts.recreate
    }

    task = {
        "collector": False, 
        "kompressor": False, 
        "aladin": False, 
        "split": True, 
        "acceptance": False, 
        "efficiency": False
    }

    aladinsplit_helper.parse_input_list(pars, start_idx=0)
    aladinsplit_helper.create_condor_files(pars, task)
    aladinsplit_helper.submit_jobs()

if __name__ == '__main__':
    main()
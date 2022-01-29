import os
import argparse
import numpy as np
import helper


def main(args=None):
    parser = argparse.ArgumentParser(description='DAMPE DATA electron-selection facility')
    parser.add_argument("-l", "--list", type=str,
                        dest='list', help='Input DATA list')
    parser.add_argument("-c", "--config", type=str,
                        dest='config', help='Collector Config Directory')
    parser.add_argument("-o", "--output", type=str,
                        dest='output', help='HTC output directory')
    parser.add_argument("-m", "--learning_method", type=str,
                        dest='lm', help='TMVA learning method')
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


    # Build the list of the BDT cuts between 80% efficiency cut and 90% efficiency cut
    bdt_cut_eff_80 = 0.23
    bdt_cut_eff_90 = 0.20
    bdt_cuts = np.linearspace(bdt_cut_eff_90, bdt_cut_eff_80, 10)

    # Loop over the BDT cuts
    for bdt_cut in bdt_cuts:

        # Build the output directory
        tmp_output_dir = os.path.join(opts.output, f"bdtcut_0{str(bdt_cut)[str(bdt_cut).rfind('.')+1:]}")
        if not os.path.exists(tmp_output_dir):
            os.mkdir(tmp_output_dir)

        pars = {
            "list": opts.list,
            "output": tmp_output_dir,
            "config": opts.config,
            "lm": opts.lm,
            "files": opts.file,
            "executable": opts.executable,
            "verbose": opts.verbose,
            "recreate": opts.new,
            "bdt_cut": bdt_cut,
            "multi": True
        }

        task = {
            "collector": False, 
            "kompressor": False, 
            "aladin": False, 
            "split": False, 
            "acceptance": False, 
            "efficiency": False,
            "signal_selection": True,
            "xtrl": False,
            "selection_split": False,
            "bdt_electron_selection": False,
            "flux_bin_profile": False
        }

        signal_selection_helper.parse_input_list(pars, start_idx=0)
        signal_selection_helper.create_condor_files(pars, task)
        signal_selection_helper.submit_jobs()

if __name__ == '__main__':
    main()
import os
import helper
import argparse

def main(args=None):
    parser = argparse.ArgumentParser(description='DAMPE flux stability study')
    parser.add_argument("-d", "--data-list", type=str,
                        dest='data_list', help='Input DATA list')
    parser.add_argument("-m", "--mc-list", type=str,
                        dest='mc_list', help='Input MC list')
    parser.add_argument("-p", "--proton_fit_file", type=str,
                        dest='proton_fit_file', help='Proton fit file')

    parser.add_argument("-c", "--config", type=str,
                        dest='config', help='Energy Config File')
    parser.add_argument("-b", "--bdt-config", type=str,
                        dest='bdt_config', help='BDT Config File')
    parser.add_argument("-a", "--acceptance", type=str,
                        dest='acceptance', help='Acceptance ROOT File')
    parser.add_argument("-e", "--exposure", type=str,
                        dest='exposure', help='Exposure')
    parser.add_argument("-f", "--eff-corr-func", type=str,
                        dest='eff_corr_func', help='Efficiency Correction Functions')
    parser.add_argument("-o", "--output", type=str,
                        dest='output', help='HTC output directory')
    parser.add_argument("-x", "--executable", type=str,
                        dest='executable', help='Analysis script')
    parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                        action='store_true', help='run in high verbosity mode')
    parser.add_argument("-n", "--new", dest='new', default=False,
                        action='store_true', help='recreate output dirs if present')

    opts = parser.parse_args(args)
    flux_bin_profile_helper = helper.helper()

    bins = 50

    for bin in range(1, bins+1):
        
        # Build the output directory
        tmp_output_dir = os.path.join(opts.output, f"energy_bin_{bin}")
        if not os.path.exists(tmp_output_dir):
            os.mkdir(tmp_output_dir)

        pars = {
            "list": opts.data_list,
            "mc_list": opts.mc_list,
            "proton_fit_file": opts.proton_fit_file,
            "energy_config_file": opts.config,
            "bdt_config_file": opts.bdt_config,
            "acceptance_file": opts.acceptance,
            "exposure": opts.exposure,
            "energy_bin": bin,
            "learning_method": "BDT",
            "eff_corr_functions": opts.eff_corr_func,
            "output": tmp_output_dir,
            "files": 1e+6,
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
            "signal_selection": False,
            "xtrl": False,
            "selection_split": False,
            "bdt_electron_selection": False,
            "flux_bin_profile": True,
            "preselection": False
        }

        flux_bin_profile_helper.parse_input_list(pars, start_idx=0)
        flux_bin_profile_helper.create_condor_files(pars, task)
        flux_bin_profile_helper.submit_jobs()
        flux_bin_profile_helper.reset()

if __name__ == '__main__':
    main()
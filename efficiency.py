import helper
import argparse

def main(args=None):
    parser = argparse.ArgumentParser(description='DAMPE Efficiency facility')
    parser.add_argument("-l", "--list", type=str,
                        dest='list', help='Input DATA/MC list')
    
    parser.add_argument("-b", "--config-bdt", type=str,
                        dest='config_bdt', help='BDT Config File')
    parser.add_argument("-s", "--signal-correction", type=str,
                        dest='signal_correction', help='Signal Correction ROOT File')
    parser.add_argument("-e", "--config-energy", type=str,
                        dest='config_energy', help='Energy Config File')
    parser.add_argument("-m", "--learning-method", type=str,
                        dest='learning_method', help='TMVA Learning Method')
    parser.add_argument("-c", "--cosine-regularize", type=str,
                        dest='cosine_regularize', help='Cosine Regularize Correction ROOT File')
    parser.add_argument("-t", "--box-cox-regularize", type=str,
                        dest='box_cox_regularize', help='Box-Cox Regularize Correction ROOT File')
    

    parser.add_argument("-o", "--output", type=str,
                        dest='output', help='HTC output directory')
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
        "config-bdt": opts.config_bdt,
        "signal-correction": opts.signal_correction,
        "config-energy": opts.config_energy,
        "learning-method": opts.learning_method,
        "cosine-regularize": opts.cosine_regularize,
        "box-cox-regularize": opts.box_cox_regularize,
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
        "bdt_electron_selection": False,
        "flux_bin_profile": False
    }

    efficiency_helper.parse_input_list(pars, start_idx=0)
    efficiency_helper.create_condor_files(pars, task)
    efficiency_helper.submit_jobs()

if __name__ == '__main__':
    main()
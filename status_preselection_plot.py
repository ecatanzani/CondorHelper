import helper
import argparse

def main(args=None):
    parser = argparse.ArgumentParser(description='Status check facility')

    parser.add_argument("-i", "--input", type=str,
                        dest='input', help='Input DATA/MC folder')
    parser.add_argument("-r", "--resubmit", dest='resubmit', default=False,
                            action='store_true', help='HTCondor flag to resubmit failed jobs')
    parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                        action='store_true', help='run in high verbosity mode')

    opts = parser.parse_args(args)
    status_helper = helper.helper()

    pars = {
        "input": opts.input,
        "resubmit": opts.resubmit,
        "verbose": opts.verbose
    }

    status_helper.status_preselection_stats(pars)


if __name__ == '__main__':
    main()
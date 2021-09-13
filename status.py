import helper
import argparse

def main(args=None):
    parser = argparse.ArgumentParser(description='Status check facility')

    parser.add_argument("-i", "--input", type=str,
                        dest='input', help='Input DATA/MC folder')
    parser.add_argument("-r", "--resubmit", dest='resubmit', default=False,
                            action='store_true', help='HTCondor flag to resubmit failed jobs')
    parser.add_argument("-l", "--list", dest='list', default=False,
                            action='store_true', help='Produce file list')
    parser.add_argument("-s", "--split", dest='split', default=False,
                            action='store_true', help='Split file list by energy bin')
    parser.add_argument("-m", "--modify_sub_file", type=str,
                        dest='modify_sub_file', default="cndr.sub", help='HTCondor sub file name')
    parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                        action='store_true', help='run in high verbosity mode')

    opts = parser.parse_args(args)
    aladin_helper = helper.helper()

    pars = {
        "input": opts.input,
        "resubmit": opts.resubmit,
        "list": opts.list,
        "split": opts.split,
        "modify_sub_file": opts.modify_sub_file,
        "verbose": opts.verbose
    }

    aladin_helper.status(pars)


if __name__ == '__main__':
    main()
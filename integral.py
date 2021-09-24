import helper
import argparse

def main(args=None):
    parser = argparse.ArgumentParser(description='Integral facility')

    parser.add_argument("-i", "--input", type=str,
                        dest='input', help='Input condor jobs WD')
    parser.add_argument("-o", "--output", type=str,
                            dest='output', help='output ROOT file')
    parser.add_argument("-f", "--filter", type=str,
                            dest='filter', help='File name to add')
    parser.add_argument("-b", "--bin_order", dest='bin_order', default=False,
                            action='store_true', help='integral for aladin facility')
    parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                        action='store_true', help='run in high verbosity mode')

    opts = parser.parse_args(args)
    integral_helper = helper.helper()

    pars = {
        "input": opts.input,
        "output": opts.output,
        "filter": opts.filter,
        "bin_order": opts.bin_order,
        "verbose": opts.verbose
    }

    integral_helper.integral(pars)


if __name__ == '__main__':
    main()
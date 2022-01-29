import os
import ROOT
from tqdm import tqdm
import subprocess
from argparse import ArgumentParser

def main(args=None):
    parser = ArgumentParser(description='DAMPE electron-selection facility')

    parser.add_argument("-l", "--list", type=str,
                        dest='list', help='Input MC list')
    parser.add_argument("-f", "--filter", type=str,
                        dest='config', help='TTree filter')
    parser.add_argument("-o", "--output", type=str,
                        dest='output', help='Output TFile')

    opts = parser.parse_args(args)

    with open(opts.list) as file_list:
        files = file_list.read().splitlines()

    os.mkdir('tmp')
    evt = 0

    for file in tqdm(files, desc=f"Scanning TTrees..."):
        rdf = ROOT.RDataFrame('electron_tree', file)
        rdf.Snapshot('electron_tree', os.path.join('tmp', f'proton_background_tree_{evt}.root'))
        evt += 1

    forest = [os.path.join('tmp', file) for file in os.listdir('tmp') if file.endswith('.root')]
    _file_list = str()
    for tree in forest:
        _file_list += f" {tree}"
    
    _cmd = f"hadd {opts.output}{_file_list}"
    print(_cmd)
    subprocess.run(_cmd, shell=True, check=True)



if __name__ == '__main__':
    main()
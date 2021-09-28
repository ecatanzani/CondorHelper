import os
import shutil
import subprocess
from tqdm import tqdm
class helper():

    def __init__(self):
        self.condorDirs = []
        self.skipped_dirs = []
        self.data_dirs = []
        self.data_files = []
        self.skipped_file_notFinalDir = 0
        self.skipped_file_notAllOutput = 0
        self.skipped_file_noSingleROOTfile = 0
        self.skipped_file_notROOTfile = 0
        self.skipped_file_notReadable = 0
        self.skipped_file_noKeys = 0
        self.ntuples_file_size = 6

    def parse_input_list(self, pars: dict, start_idx: int = 0, recursive: bool = False):
        out_dir = pars['output']
        with open(pars['list'], 'r') as inputList:
            list_idx = start_idx
            data_list = []
            for file_name in inputList:
                if not file_name.startswith('.'):
                    data_list.append(file_name.rstrip('\n'))
                    if len(data_list) == pars['files']:
                        _dir_data = self.write_list_to_file(data_list, f"{out_dir}/job_{list_idx}", recursive, pars['recreate'], pars['verbose'])
                        if _dir_data[0]:
                            self.condorDirs.append(_dir_data[1])
                        list_idx += 1
                        data_list.clear()
            if data_list:
                _dir_data = self.write_list_to_file(data_list, f"{out_dir}/job_{list_idx}", recursive, pars['recreate'], pars['verbose'])
                if _dir_data[0]:
                    self.condorDirs.append(_dir_data[1])
                list_idx += 1
                data_list.clear()

    def write_list_to_file(self, data_list: list, tmp_dir_name: str, recursive: bool = False, recreate: bool = False, verbose: bool = True) -> tuple:
        good_dir = False
        
        if not os.path.isdir(tmp_dir_name):
            try:
                os.mkdir(tmp_dir_name)
            except OSError:
                print(f"Creation of the output directory {tmp_dir_name} failed")
                raise
            else:
                good_dir = True
                if verbose:
                    print(f"Succesfully created output directory: {tmp_dir_name}")
        else:
            if recreate:
                if verbose:
                    print(f"Using existing output path directory: {tmp_dir_name}")
                    print("The directory, containing the following files, will be deleted...")
                    print(os.listdir(tmp_dir_name))
                shutil.rmtree(tmp_dir_name)
                os.mkdir(tmp_dir_name)
                good_dir = True
            else:
                if verbose:
                    print(f"Found existing output path directory: {tmp_dir_name} ... exit")
                good_dir = False
        if good_dir:
            list_path = f"{tmp_dir_name}/dataList.txt"
            try:
                with open(list_path, 'w') as out_tmp_list:
                    if recursive:
                        file_list = []
                        for sub_list in data_list:
                            with open(sub_list, "r") as _file:
                                lines = _file.read().splitlines()
                                lines.sort()
                                file_list += lines
                        data_list = file_list

                    for idx, file in enumerate(data_list):
                        if not idx:
                            out_tmp_list.write(file)
                        else:
                            out_tmp_list.write(f"\n{file}")
            except OSError:
                print(f"Creation of the output data list {list_path} failed")
                raise
            else:
                if verbose:
                    print(f"Created output list: {list_path}")
        return (good_dir, tmp_dir_name)

    def TestROOTFile(self, path):
        from ROOT import TFile
        _tmp_file = TFile(path)
        if _tmp_file and not _tmp_file.IsOpen():
            return False
        elif _tmp_file and _tmp_file.IsOpen() and _tmp_file.IsZombie():
            _tmp_file.Close()
            return False
        elif _tmp_file and _tmp_file.IsOpen() and _tmp_file.TestBit(TFile.kRecovered):
            _tmp_file.Close()
            return False
        else:
            _tmp_file.Close()
            return True

    def getListOfFiles(self, condor_wd: str):
        from ROOT import TFile

        # Starting loop on output condor dirs
        for tmp_dir in tqdm(os.listdir(condor_wd), desc='Scanning local HTCondor dir'):
            if tmp_dir.startswith('job_'):
                full_dir_path = f"{condor_wd}/{tmp_dir}"
                expected_condor_outDir = f"{full_dir_path}/outFiles"
                # Check if 'outFiles' dir exists
                if os.path.isdir(expected_condor_outDir):
                    _list_dir = [f"{expected_condor_outDir}/{file}" for file in os.listdir(expected_condor_outDir) if file.endswith('.root')]
                    skipped_dir = False
                    for file_idx, tmp_acc_full_path in enumerate(_list_dir):
                        # Check if output ROOT file exists
                        if os.path.isfile(tmp_acc_full_path):
                            # Check if output ROOT file is redable
                            if self.TestROOTFile(tmp_acc_full_path):
                                tmp_acc_file = TFile.Open(tmp_acc_full_path, "READ")
                                # Check if output ROOT file is redable
                                if tmp_acc_file.IsOpen():
                                    # Check if output ROOT file has keys
                                    outKeys = tmp_acc_file.GetNkeys()
                                    if outKeys:
                                        if file_idx == len(_list_dir)-1 and not skipped_dir:
                                            self.data_dirs.append(full_dir_path)
                                        self.data_files.append(tmp_acc_full_path)
                                    else:
                                        # output ROOT file has been open but has not keys
                                        if not skipped_dir:
                                            self.skipped_dirs.append(full_dir_path)
                                            skipped_dir = True
                                        self.skipped_file_noKeys += 1
                            else:
                                # output ROOT file has not been opened correctly
                                if not skipped_dir:
                                    self.skipped_dirs.append(full_dir_path)
                                    skipped_dir = True
                                self.skipped_file_notReadable += 1
                        else:
                            # output ROOT file does not exist
                            if not skipped_dir:
                                self.skipped_dirs.append(full_dir_path)
                                skipped_dir = True
                            self.skipped_file_notROOTfile += 1
                else:
                    # 'outFiles' dir does not exists
                    self.skipped_dirs.append(full_dir_path)
                    self.skipped_file_notFinalDir += 1

    def cleanListOfFiles(self):
        for file in self.data_files:
            if self.sub_opts.filter not in file:
                self.data_files.remove(file)

    def orderListOfFiles(self):
        ordered_list = []
        for bin_idx in range(0, len(self.data_files)):
            for file in self.data_files:
                if f"job_{bin_idx}/" in file:
                    ordered_list.append(file)
        self.data_files = ordered_list

    def clean_condor_dir(self, dir: str):
        os.chdir(dir)
        outCondor = [filename for filename in os.listdir('.') if filename.startswith("out")]
        # Clean the job dir
        if outCondor:
            for elm in outCondor:
                if os.path.isdir(elm):
                    shutil.rmtree(elm)
                if os.path.isfile(elm):
                    os.remove(elm)

    def resubmit_condor_jobs(self, verbose: bool, sub_file: str = "cndr.sub"):
        for dir in self.skipped_dirs:
            self.clean_condor_dir(dir)

            # Submit HTCondor job
            if verbose:
                print(f"Resubmitting job from folder: {dir}")

            subprocess.run(f"condor_submit -name sn-01.cr.cnaf.infn.it -spool {sub_file}", shell=True, check=True)

    def checkargs(self, task: dict) -> bool:
        if sum(task.values()) == 1:
            return True
        else:
            return False

    def create_condor_files(self, pars: dict, task: dict, mt: bool = False):
        if self.checkargs(task):
            for cDir in self.condorDirs:

                # Find out paths
                outputPath = f"{cDir}/output.log"
                logPath = f"{cDir}/output.clog"
                errPath = f"{cDir}/output.err"
                bashScriptPath = f"{cDir}/script.sh"

                # Writing sub file
                subFilePath = f"{cDir}/cndr.sub"
                try:
                    with open(subFilePath, 'w') as outSub:
                        outSub.write("universe = vanilla\n")
                        if task['kompressor'] or task['aladin'] or task['split'] or task['acceptance']:
                            if mt:
                                outSub.write("request_cpus = 4\n")
                                outSub.write("request_memory = 4096\n")
                        outSub.write(f"executable = {bashScriptPath}\n")
                        outSub.write(f"output = {outputPath}\n")
                        outSub.write(f"error = {errPath}\n")
                        outSub.write(f"log = {logPath}\n")
                        outSub.write("ShouldTransferFiles = YES\n")
                        outSub.write("WhenToTransferOutput = ON_EXIT\n")
                        outSub.write("queue 1")
                except OSError:
                    print(f"ERROR creating HTCondor sub file in: {cDir}")
                    raise
                else:
                    if pars['verbose']:
                        print(f"HTCondor sub file created in: {cDir}")

                # Build executable bash script
                dataListPath = f"{cDir}/dataList.txt"
                try:
                    with open(bashScriptPath, "w") as outScript:
                        if task['collector']:
                            self.collector_task(outScript, dataListPath, cDir, pars)
                        if task['kompressor']:
                            self.kompressor_task(outScript, dataListPath, cDir, pars)
                        if task['aladin']:
                            self.aladin_task(outScript, dataListPath, cDir, pars)
                        if task['split']:
                            self.split_task(outScript, dataListPath, cDir, pars)
                        if task['acceptance']:
                            self.acceptance_task(outScript, dataListPath, cDir, pars)
                        if task['efficiency']:
                            self.efficiency_task(outScript, dataListPath, cDir, pars)
                except OSError:
                    print(f"ERROR creating HTCondor bash script file in: {cDir}")
                    raise
                else:
                    if pars['verbose']:
                        print(f"HTCondor bash script file created in: {cDir}")

                # Make bash script executable
                subprocess.run(f"chmod +x {bashScriptPath}", shell=True, check=True)

    def submit_jobs(self):
        for folder in self.condorDirs:
            subFilePath = f"{folder}/cndr.sub"
            subprocess.run([f"condor_submit -name sn-01.cr.cnaf.infn.it -spool {subFilePath}"], shell=True, check=True)

    def collector_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        tmpOutDir = f"{cDir}/outFiles"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
        outScript.write("dampe_init trunk\n")
        outScript.write(f"mkdir {tmpOutDir}\n")
        _cmd = f"{pars['executable']} -w {pars['config']} -i {dataListPath} -d {tmpOutDir} -v "
        _cmd += "-m" if pars['mc'] else "-r"
        outScript.write(_cmd)

    def kompressor_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        tmpOutDir = f"{cDir}/outFiles"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"mkdir {tmpOutDir}\n")

        _opt_command = ""
        if pars['config']:
            _opt_command += f"-w {pars['config']} "
        if pars['mc']:
            _opt_command += "-m "
        if pars['regularize']:
            _opt_command += f"-r {pars['regularize']} "

        _command = f"{pars['executable']} -i {dataListPath} -d {tmpOutDir} -v {_opt_command}"

        outScript.write(_command)

    def get_energy_bin(self, dataListPath):
        with open(dataListPath, "r") as _list:
            line = _list.readline()
        return line[line.rfind('_')+1:line.rfind('.root')]    

    def aladin_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        tmpOutDir = f"{cDir}/outFiles"
        ldpath = f"{pars['executable'][:pars['executable'].rfind('Aladin/')+7]}dylib"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"export LD_LIBRARY_PATH={ldpath}:$LD_LIBRARY_PATH\n")
        outScript.write(f"mkdir {tmpOutDir}\n")
        
        _opt_command = ""
        if pars['config']:
            _opt_command += f"-w {pars['config']} "
        if pars['mc']:
            _opt_command += "-m "
        if pars['regularize']:
            _opt_command += f"-r {pars['regularize']} "
        if pars['gaussianize']:
            _opt_command += "-g "
        if pars['likelihood']:
            _opt_command += "-l "
            _opt_command += f"-b {self.get_energy_bin(dataListPath)} "
        if pars['fit']:
            _opt_command += "-f "
            _opt_command += f"-b {self.get_energy_bin(dataListPath)} "
        if pars['tmva']:
            _opt_command += f"-t {pars['tmva']}"

        _command = f"{pars['executable']} -i {dataListPath} -d {tmpOutDir} -v {_opt_command}"

        outScript.write(_command)

    def split_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        tmpOutDir = f"{cDir}/outFiles"
        ldpath = f"{pars['executable'][:pars['executable'].rfind('Split/')+6]}dylib"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"export LD_LIBRARY_PATH={ldpath}:$LD_LIBRARY_PATH\n")
        outScript.write(f"mkdir {tmpOutDir}\n")

        _opt_command = ""
        if pars['config']:
            _opt_command += f"-w {pars['config']} "
        if pars['mc']:
            _opt_command += "-m "

        _command = f"{pars['executable']} -i {dataListPath} -d {tmpOutDir} -v {_opt_command}"

        outScript.write(_command)

    def acceptance_task(self, outScript, dataListPath, cDir):
        '''
        tmpOutDir = cDir + str("/outFiles")
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write(
            "source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"mkdir {tmpOutDir}\n")
        
        _opt_command = ""
        if self.sub_opts.config:
            _opt_command += f"-w {self.sub_opts.config} "

        _command = f"{self.sub_opts.executable} -i {dataListPath} -d {tmpOutDir} -v {_opt_command}"

        outScript.write(_command)
        '''

    def efficiency_task(self, outScript, dataListPath, cDir):
        '''
        tmpOutDir = cDir + str("/outFiles")
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write(
            "source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"mkdir {tmpOutDir}\n")
        
        _opt_command = ""
        if self.sub_opts.config:
            _opt_command += f"-w {self.sub_opts.config} "
        if self.sub_opts.mc:
            _opt_command += "-m "

        _command = f"{self.sub_opts.executable} -i {dataListPath} -d {tmpOutDir} -v {_opt_command}"

        outScript.write(_command)
        '''

    '''

    def acceptance(self):
        parser = ArgumentParser(
            description='DAMPE Acceptance facility')
        parser.add_argument("-l", "--list", type=str,
                            dest='list', help='Input MC list')
        parser.add_argument("-c", "--config", type=str,
                            dest='config', help='Software Config Directory')
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

        args = parser.parse_args(sys.argv[2:])
        self.sub_opts = args
       
        self.parse_input_list(start_idx=0, recursive=False)
        self.create_condor_files(
            collector=False, kompressor=False, aladin=False, split=False, acceptance=True, efficiency=False, mt=False)
        self.submit_jobs()

    def efficiency(self):
        parser = ArgumentParser(
            description='DAMPE Efficiency facility')
        parser.add_argument("-l", "--list", type=str,
                            dest='list', help='Input MC list')
        parser.add_argument("-c", "--config", type=str,
                            dest='config', help='Software Config Directory')
        parser.add_argument("-o", "--output", type=str,
                            dest='output', help='HTC output directory')
        parser.add_argument("-m", "--mc", dest='mc',
                            default=False, action='store_true', help='MC event collector')
        parser.add_argument("-f", "--file", type=int, dest='file',
                            const=10, nargs='?', help='files to process in job')
        parser.add_argument("-x", "--executable", type=str,
                            dest='executable', help='Analysis script')
        parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                            action='store_true', help='run in high verbosity mode')
        parser.add_argument("-n", "--new", dest='new', default=False,
                            action='store_true', help='recreate output dirs if present')

        args = parser.parse_args(sys.argv[2:])
        self.sub_opts = args
       
        self.parse_input_list(start_idx=0, recursive=False)
        self.create_condor_files(
            collector=False, kompressor=False, aladin=False, split=False, acceptance=False, efficiency=True, mt=False)
        self.submit_jobs()
    '''

    def status(self, pars: dict):
        
        self.getListOfFiles(pars['input'])
        if pars['verbose']:
            print(f"Found {len(self.data_dirs)} GOOD condor directories")
        if self.skipped_dirs:
            print(f"Found {len(self.skipped_dirs)} BAD condor directories...\n")
            print(f"Found {self.skipped_file_notFinalDir} directories with no output folder")
            print(f"Found {self.skipped_file_notAllOutput} directories with inconsistent number of output ROOT files")
            print(f"Found {self.skipped_file_notROOTfile} directories with no output ROOT file")
            print(f"Found {self.skipped_file_notReadable} directories with corrupted output ROOT file")
            print(f"Found {self.skipped_file_noKeys} directories where output ROOT file has no keys\n")
            print('Here the folders list...\n')

            for idx, elm in enumerate(self.skipped_dirs):
                print(f'Skipped directory [{idx+1}] : {elm}')    

            if pars['resubmit']:
                print(f"\nResubmitting HTCondor jobs for {len(self.skipped_dirs)} directories\n")
                for dir in self.skipped_dirs:
                    self.clean_condor_dir(dir)
                self.resubmit_condor_jobs(pars['verbose'], pars['modify_sub_file'])
        else:
            if pars['list']:
                if pars['split']:
                    _single_job = [int(file[file.rfind('_')+1:file.rfind('.')]) for file in self.data_files if "job_0" in file]
                    _single_job.sort()
                    energy_nbins = _single_job[-1]
                    _single_bin_lists = []
                    for bin_idx in range(1, energy_nbins+1):
                        tmp_bin_files = [
                            file for file in self.data_files if f"energybin_{bin_idx}.root" in file]
                        _list_path = pars['input'][pars['input'].rfind('/')+1:] + f"_energybin_{bin_idx}.txt"
                        _single_bin_lists.append(f"{os.getcwd()}/{_list_path}")
                        with open(_list_path, "w") as _final_list:
                            for elm in tmp_bin_files:
                                _final_list.write(f"{elm}\n")
                    _list_path = pars['input'][pars['input'].rfind('/')+1:] + "_energybin_all_lists.txt"
                    with open(_list_path, "w") as _final_list:
                        for elm in _single_bin_lists:
                            _final_list.write(f"{elm}\n")
                else:
                    _list_path = pars['input'][pars['input'].rfind('/')+1:] + ".txt"
                    with open(_list_path, "w") as _final_list:
                        for elm in self.data_files:
                            _final_list.write(f"{elm}\n")

    def aggregate(self, pars: dict):
        if pars['verbose']:
            print(f"Going to add {len(self.data_files)} ROOT files...")
        _k_step = pars['number']
        _file_list = str()
        _list_idx = 0
        _tmp_out_name = pars['output'][:pars['output'].rfind('.')]
        for fidx, file in enumerate(self.data_files):
            _file_list += f" {file}"
            if (fidx+1) % _k_step == 0:
                _out_full_name = f"{_tmp_out_name}_{_list_idx}.root"
                _cmd = f"hadd {_out_full_name}{_file_list}"
                if pars['verbose']:
                    print(_cmd)
                subprocess.run(_cmd, shell=True, check=True)
                _file_list = f" {_out_full_name}"
                _list_idx += 1
        if _file_list:
            _out_full_name = f"{_tmp_out_name}_{_list_idx}.root"
            if pars['verbose']:
                print(_cmd)
            _cmd = f"hadd {_out_full_name}{_file_list}"
            subprocess.run(_cmd, shell=True, check=True)

    def integral(self, pars: dict):
        self.getListOfFiles(pars['input'])
        if pars['filter']:
            self.cleanListOfFiles()
        if pars['bin_order']:
            self.orderListOfFiles()
        self.aggregate(pars)
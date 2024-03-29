import os
import random
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

    def reset(self):
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
        
    def get_data_list(self, dataListPath):
        with open(dataListPath, "r") as _list:
            lines = _list.read().splitlines()
        random.shuffle(lines)
        return lines

    def parse_input_list(self, pars: dict, start_idx: int = 0, recursive: bool = False):
        out_dir = pars['output']
        list_idx = start_idx
        data_list = []
        for file_name in self.get_data_list(pars['list']):
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

    def getListOfFiles(self, condor_wd: str, folder_starts_with: str = "job_"):
        from ROOT import TFile

        # Starting loop on output condor dirs
        for tmp_dir in tqdm(os.listdir(condor_wd), desc='Scanning local HTCondor dir'):
            if tmp_dir.startswith(folder_starts_with):
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

    def getListOfFiles_FluxProfile(self, condor_wd: str):
        from ROOT import TFile

        # Starting loop on output condor dirs
        for bin_dir in tqdm(os.listdir(condor_wd), desc='Scanning local HTCondor dir'):
            for tmp_dir in os.listdir(os.path.join(condor_wd, bin_dir)):
                full_dir_path = os.path.join(condor_wd, bin_dir, tmp_dir)
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
    
    def getPartialListOfFiles(self, condor_wd: str):
        from ROOT import TFile

        # Starting loop on output condor dirs
        for tmp_dir in tqdm(os.listdir(condor_wd), desc='Scanning local HTCondor dir'):
            if tmp_dir.startswith('job_'):
                full_dir_path = f"{condor_wd}/{tmp_dir}"
                if "output.log" in os.listdir(full_dir_path):
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

    def cleanListOfFiles(self, pars: dict):
        for file in self.data_files:
            if pars['filter'] not in file[file.rfind('/')+1:]:
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

            #subprocess.run(f"condor_submit -name sn-01.cr.cnaf.infn.it -spool {sub_file}", shell=True, check=True)
            subprocess.run(f"condor_submit -name sn-02.cr.cnaf.infn.it -spool {sub_file}", shell=True, check=True)

    def checkargs(self, task: dict) -> bool:
        if sum(task.values()) == 1:
            return True
        else:
            return False

    def create_bdt_config_file(self, bdt_cut: float, path: str):
    
        # BDT weights paths are fixed
        low_energy_weights = "/storage/gpfs_data/dampe/users/ecatanzani/myRepos/DAMPE/eFlux/Classifier/Reader/trainedBDT/10GeV_100GeV/TMVAClassification_BDT.weights.xml"        
        medium_energy_weights = "/storage/gpfs_data/dampe/users/ecatanzani/myRepos/DAMPE/eFlux/Classifier/Reader/trainedBDT/100GeV_1TeV/TMVAClassification_BDT.weights.xml"
        high_energy_weights = "/storage/gpfs_data/dampe/users/ecatanzani/myRepos/DAMPE/eFlux/Classifier/Reader/trainedBDT/1TeV_10TeV/TMVAClassification_BDT.weights.xml"

        #Build output config file
        with open(os.path.join(path, "classifier.conf")) as classifier_config_file:
            classifier_config_file.write("################### Classifier Config File ###################\n\n")
            classifier_config_file.write(f"low_energy_weights {low_energy_weights}\n")
            classifier_config_file.write(f"medium_energy_weights {medium_energy_weights}\n")
            classifier_config_file.write(f"high_energy_weights {high_energy_weights}\n\n")
            classifier_config_file.write(f"low_energy_classifier_cut {bdt_cut}\n")
            classifier_config_file.write(f"medium_energy_classifier_cut {bdt_cut}\n")
            classifier_config_file.write(f"high_energy_classifier_cut {bdt_cut}")

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

                # Write BDT classifier config file
                if task['signal_selection'] and pars['multi']:
                    self.create_bdt_config_file(pars['bdt_cut'], cDir)

                # Build executable bash script
                dataListPath = f"{cDir}/dataList.txt"
                try:
                    with open(bashScriptPath, "w") as outScript:
                        if task['collector']:
                            self.collector_task(outScript, dataListPath, cDir, pars)
                        if task['preselection']:
                            self.preselection_task(outScript, dataListPath, cDir, pars)
                        elif task['kompressor']:
                            self.kompressor_task(outScript, dataListPath, cDir, pars)
                        elif task['aladin']:
                            self.aladin_task(outScript, dataListPath, cDir, pars)
                        elif task['split']:
                            self.split_task(outScript, dataListPath, cDir, pars)
                        elif task['acceptance']:
                            self.acceptance_task(outScript, dataListPath, cDir, pars)
                        elif task['efficiency']:
                            self.efficiency_task(outScript, dataListPath, cDir, pars)
                        elif task['signal_selection']:
                            self.signal_selection_task(outScript, dataListPath, cDir, pars)
                        elif task['xtrl']:
                            self.xtrl_task(outScript, dataListPath, cDir, pars)
                        elif task['selection_split']:
                            self.selection_split_task(outScript, dataListPath, cDir, pars)
                        elif task['bdt_electron_selection']:
                            self.bdt_electron_selection_task(outScript, dataListPath, cDir, pars)
                        elif task['flux_bin_profile']:
                            self.flux_bin_profile(outScript, dataListPath, cDir, pars)
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
            #subprocess.run([f"condor_submit -name sn-01.cr.cnaf.infn.it -spool {subFilePath}"], shell=True, check=True)
            subprocess.run([f"condor_submit -name sn-02.cr.cnaf.infn.it -spool {subFilePath}"], shell=True, check=True)

    def collector_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        tmpOutDir = f"{cDir}/outFiles"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
        outScript.write("dampe_init trunk\n")
        outScript.write(f"mkdir {tmpOutDir}\n")
        _cmd = f"{pars['executable']} -w {pars['config']} -i {dataListPath} -d {tmpOutDir} -v "
        _cmd += "-m" if pars['mc'] else "-r"
        outScript.write(_cmd)

    def preselection_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        tmpOutDir = f"{cDir}/outFiles"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"mkdir {tmpOutDir}\n")
        _cmd = f"{pars['executable']} -i {dataListPath} -e {pars['config']} -s {pars['spectral']} -d {tmpOutDir} -v "
        if pars['mc']:
            _cmd += "-m"
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
        if not pars['aladin_tmva']:
            self.aladin_plain_task(outScript, dataListPath, cDir, pars)
        else:
            self.aladin_tmva_task(outScript, dataListPath, cDir, pars)

    def aladin_plain_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
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

    def aladin_tmva_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        tmpOutDir = f"{cDir}/outFiles"
        ldpath = f"{pars['executable'][:pars['executable'].rfind('CreateSets/')+11]}dylib"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"export LD_LIBRARY_PATH={ldpath}:$LD_LIBRARY_PATH\n")
        outScript.write(f"mkdir {tmpOutDir}\n")
        
        _opt_command = ""
        if pars['config']:
            _opt_command += f"-w {pars['config']} "
        if not pars['split']:
            _opt_command += "-n "   

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

    def acceptance_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        
        tmpOutDir = f"{cDir}/outFiles"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"mkdir {tmpOutDir}\n")
        
        _opt_command = ""
        if pars['config']:
            _opt_command += f"-w {pars['config']} "

        _command = f"{pars['executable']} -i {dataListPath} -d {tmpOutDir} -v {_opt_command}"

        outScript.write(_command)
        

    def efficiency_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        
        tmpOutDir = f"{cDir}/outFiles"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"mkdir {tmpOutDir}\n")
        
        _command = f"{pars['executable']} -i {dataListPath} -d {tmpOutDir} -v -b {pars['config-bdt']} -f {pars['signal-correction']} -e {pars['config-energy']} -l {pars['learning-method']} -c {pars['cosine-regularize']} -t {pars['box-cox-regularize']}"

        outScript.write(_command)
        

    def xtrl_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        
        tmpOutDir = f"{cDir}/outFiles"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"mkdir {tmpOutDir}\n")

        _command = f"{pars['executable']} -i {dataListPath} -w {pars['config']} -d {tmpOutDir} -v"

        outScript.write(_command)

    def signal_selection_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        
        tmpOutDir = f"{cDir}/outFiles"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"mkdir {tmpOutDir}\n")

        if pars['multi']:
            _command = f"{pars['executable']} -i {dataListPath} -c {pars['config']} -b {os.path.join(cDir, 'classifier.conf')} -m {pars['lm']} -d {tmpOutDir} -v"
        else:
            _command = f"{pars['executable']} -i {dataListPath} -c {pars['config']} -b {pars['bdt_config']} -m {pars['lm']} -d {tmpOutDir} -v"

        outScript.write(_command)

    def selection_split_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        
        tmpOutDir = f"{cDir}/outFiles"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"mkdir {tmpOutDir}\n")

        _command = f"{pars['executable']} -i {dataListPath} -c {pars['config']} -o {tmpOutDir} -v"

        outScript.write(_command)

    def bdt_electron_selection_task(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        
        tmpOutDir = f"{cDir}/outFiles"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"mkdir {tmpOutDir}\n")

        _opt_command = ""
        if pars['eff_corr']:
            _opt_command += f"-f {pars['eff_corr']} "
            
        _command = f"{pars['executable']} -i {dataListPath} -c {pars['config']} -d {tmpOutDir} -v {_opt_command}"

        outScript.write(_command)

    def flux_bin_profile(self, outScript: str, dataListPath: str, cDir: str, pars: dict):
        
        tmpOutDir = f"{cDir}/outFiles"
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /opt/rh/devtoolset-7/enable\n")
        outScript.write("source /storage/gpfs_data/dampe/users/ecatanzani/deps/root-6.22/bin/thisroot.sh\n")
        outScript.write(f"mkdir {tmpOutDir}\n")

        _command = f"{pars['executable']} -i {dataListPath} -e {pars['mc_list']} -p {pars['proton_fit_file']} -c {pars['energy_config_file']} -b {pars['bdt_config_file']} -a {pars['acceptance_file']} -t {pars['exposure']} -r {pars['energy_bin']} -f {pars['eff_corr_functions']} -m {pars['learning_method']} -d {tmpOutDir} -v"

        outScript.write(_command)

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

    def status_preselection_stats(self, pars: dict):
        
        self.getListOfFiles(pars['input'], folder_starts_with="20")
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
                self.resubmit_condor_jobs(pars['verbose'])

    def status_flux_profile(self, pars: dict):
        
        self.getListOfFiles_FluxProfile(pars['input'])
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
            _cmd = f"hadd {_out_full_name}{_file_list}"
            if pars['verbose']:
                print(_cmd)
            subprocess.run(_cmd, shell=True, check=True)

    def integral(self, pars: dict):
        self.getListOfFiles(pars['input'])
        if pars['filter']:
            self.cleanListOfFiles(pars)
        if pars['bin_order']:
            self.orderListOfFiles()
        self.aggregate(pars)
    
    def integral_flux_profile(self, pars: dict):
        self.getListOfFiles_FluxProfile(pars['input'])
        if pars['filter']:
            self.cleanListOfFiles(pars)
        if pars['bin_order']:
            self.orderListOfFiles()
        self.aggregate(pars)

    def integral_partial(self, pars: dict):
        self.getPartialListOfFiles(pars['input'])
        if pars['filter']:
            self.cleanListOfFiles(pars)
        if pars['bin_order']:
            self.orderListOfFiles()
        if pars['list']:
            _list_path = pars['input'][pars['input'].rfind('/')+1:] + ".txt"
            with open(_list_path, "w") as _final_list:
                for elm in self.data_files:
                    _final_list.write(f"{elm}\n")
        else:
            self.aggregate(pars)
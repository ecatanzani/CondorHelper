import os
import sys
import shutil
import subprocess
from argparse import ArgumentParser


class dampe_helper():

    def __init__(self):
        self.condorDirs = []
        self.sub_opts = None
        self.years_content = {}
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
        parser = ArgumentParser(
                description='Condor Deploy Helper',
                usage='''<command> [<args>]

	The most commonly used commands are:
		** DAMPE **
		collector   	call DAMPE all-electron flux facility
		status		call DAMPE HTCondor status facility
        cargo		move ROOT files to final destination
	''')

        parser.add_argument('command', help='Subcommand to run')
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print("Unrecognized command")
            parser.print_help()
            exit(1)
        getattr(self, args.command)()

    def parse_input_list(self):
        out_dir = self.sub_opts.output
        with open(self.sub_opts.list, 'r') as inputList:
            list_idx = 0
            data_list = []
            for file_name in inputList:
                if not file_name.startswith('.'):
                    data_list.append(file_name.rstrip('\n'))
                    if len(data_list) == self.sub_opts.file:
                        _tmp_out_dir = out_dir + "/" + "job_" + str(list_idx)
                        _dir_data = self.write_list_to_file(
                            data_list, _tmp_out_dir)
                        if _dir_data[0]:
                            self.condorDirs.append(_dir_data[1])
                            list_idx += 1
                        data_list.clear()
            if data_list:
                _tmp_out_dir = out_dir + "/" + "job_" + str(list_idx)
                _dir_data = self.write_list_to_file(data_list, _tmp_out_dir)
                if _dir_data[0]:
                    self.condorDirs.append(_dir_data[1])
                    list_idx += 1
                data_list.clear()

    def extract_timing_info(self):
        with open(self.sub_opts.list, 'r') as inputList:
            for filename in inputList:
                if not filename.startswith('.'):
                    year_fidx = filename.find('2A/') + 3
                    year = filename[year_fidx: year_fidx + 4]
                    month = filename[year_fidx + 4: year_fidx + 6]
                    if year not in self.years_content:
                        self.years_content[year] = {}
                    if month not in self.years_content[year]:
                        self.years_content[year][month] = []

                    self.years_content[year][month].append(
                        filename.rstrip('\n'))

    def parse_timing_info(self):
        for year in self.years_content:
            for month in self.years_content[year]:
                out_dir = self.sub_opts.output + "/" + year + month
                os.mkdir(out_dir)
                list_idx = 0
                data_list = []
                for file_name in self.years_content[year][month]:
                    data_list.append(file_name.rstrip('\n'))
                    if len(data_list) == self.sub_opts.depth:
                        _tmp_out_dir = out_dir + "/" + "job_" + str(list_idx)
                        _dir_data = self.write_list_to_file(
                            data_list, _tmp_out_dir)
                        if _dir_data[0]:
                            self.condorDirs.append(_dir_data[1])
                            list_idx += 1
                        data_list.clear()
                if data_list:
                    _tmp_out_dir = out_dir + "/" + "job_" + str(list_idx)
                    _dir_data = self.write_list_to_file(
                        data_list, _tmp_out_dir)
                    if _dir_data[0]:
                        self.condorDirs.append(_dir_data[1])
                        list_idx += 1
                    data_list.clear()

    def write_list_to_file(self, data_list, tmp_dir_name):
        good_dir = False
        if not os.path.isdir(tmp_dir_name):
            try:
                os.mkdir(tmp_dir_name)
            except OSError:
                print('Creation of the output directory {} failed'.format(tmp_dir_name))
                raise
            else:
                good_dir = True
                if self.sub_opts.verbose:
                    print('Succesfully created output directory: {}'.format(
                        tmp_dir_name))
        else:
            if self.sub_opts.verbose:
                print('Using existing output path directory: {}'.format(tmp_dir_name))
            if self.sub_opts.recreate:
                if self.sub_opts.verbose:
                    print(
                        "The directory, containing the following files, will be deleted...")
                    print(os.listdir(tmp_dir_name))
                shutil.rmtree(tmp_dir_name)
                os.mkdir(tmp_dir_name)
                good_dir = True
            else:
                good_dir = False
        if good_dir:
            list_path = tmp_dir_name + "/dataList.txt"
            try:
                with open(list_path, 'w') as out_tmp_list:
                    for idx, file in enumerate(data_list):
                        if idx == 0:
                            out_tmp_list.write(file)
                        else:
                            out_tmp_list.write("\n%s" % file)
            except OSError:
                print('Creation of the output data list {} failed'.format(list_path))
                raise
            else:
                if self.sub_opts.verbose:
                    print('Created output list: {}'.format(list_path))
        return (good_dir, tmp_dir_name)

    def create_condor_files(self):
        for cDir in self.condorDirs:

            # Find out paths
            outputPath = cDir + "/" + "output.log"
            logPath = cDir + "/" + "output.clog"
            errPath = cDir + "/" + "output.err"
            bashScriptPath = cDir + str("/script.sh")

            # Writing sub file
            subFilePath = cDir + str("/crawler.sub")
            try:
                with open(subFilePath, 'w') as outSub:
                    outSub.write("universe = vanilla\n")
                    outSub.write('executable = {}\n'.format(bashScriptPath))
                    outSub.write('output = {}\n'.format(outputPath))
                    outSub.write('error = {}\n'.format(errPath))
                    outSub.write('log = {}\n'.format(logPath))
                    outSub.write("ShouldTransferFiles = YES\n")
                    outSub.write("WhenToTransferOutput = ON_EXIT\n")
                    outSub.write("queue 1")
            except OSError:
                print('ERROR creating HTCondor sub file in: {}'.format(cDir))
                raise
            else:
                if self.sub_opts.verbose:
                    print('HTCondor sub file created in: {}'.format(cDir))

            # Build executable bash script
            dataListPath = cDir + str("/dataList.txt")
            try:
                with open(bashScriptPath, "w") as outScript:
                    self.collector_task(outScript, dataListPath, cDir)
            except OSError:
                print('ERROR creating HTCondor bash script file in: {}'.format(cDir))
                raise
            else:
                if self.sub_opts.verbose:
                    print('HTCondor bash script file created in: {}'.format(cDir))

            # Make bash script executable
            subprocess.run('chmod +x {}'.format(bashScriptPath),
                           shell=True, check=True)

    def submit_jobs(self):
        for folder in self.condorDirs:
            subFilePath = folder + str("/crawler.sub")
            subprocess.run(
                ['condor_submit -name sn-01.cr.cnaf.infn.it -spool {}'.format(subFilePath)], shell=True, check=True)

    def collector_task(self, outScript, dataListPath, cDir):
        tmpOutDir = cDir + str("/outFiles")
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
        outScript.write("dampe_init trunk\n")
        outScript.write('mkdir {}\n'.format(tmpOutDir))
        if self.sub_opts.mc:
            outScript.write('{} -w {} -i {} -d {} -m -v'.format(
                self.sub_opts.executable,
                self.sub_opts.config,
                dataListPath,
                tmpOutDir))
        if self.sub_opts.data:
            outScript.write('{} -w {} -i {} -d {} -r -v'.format(
                self.sub_opts.executable,
                self.sub_opts.config,
                dataListPath,
                tmpOutDir))

    def collector(self):
        parser = ArgumentParser(
            description='DAMPE all-electron collector facility')
        parser.add_argument("-l", "--list", type=str,
                            dest='list', help='Input DATA/MC list')
        parser.add_argument("-c", "--config", type=str,
                            dest='config', help='Software Config Directory')
        parser.add_argument("-o", "--output", type=str,
                            dest='output', help='HTC output directory')
        parser.add_argument("-m", "--mc", dest='mc',
                            default=False, action='store_true', help='MC event collector')
        parser.add_argument("-d", "--data", dest='data',
                            default=False, action='store_true', help='DATA event collector')
        parser.add_argument("-t", "--time_ntuple", dest='time_ntuple',
                            default=False, action='store_true', help='nTuple time ID')
        parser.add_argument("-f", "--file", type=int, dest='file',
                            const=100, nargs='?', help='files to process in job')
        parser.add_argument("-x", "--executable", type=str,
                            dest='executable', help='Analysis script')

        parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                            action='store_true', help='run in high verbosity mode')
        parser.add_argument("-r", "--recreate", dest='recreate', default=False,
                            action='store_true', help='recreate output dirs if present')
        args = parser.parse_args(sys.argv[2:])
        self.sub_opts = args

        if self.sub_opts.time_ntuple:
            self.extract_timing_info()
            self.parse_timing_info()
        else:
            self.parse_input_list()
        self.create_condor_files()
        self.submit_jobs()

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

    def getListOfFiles(self, condor_wd):
        from ROOT import TFile

        # Starting loop on output condor dirs
        for tmp_dir in os.listdir(condor_wd):
            if tmp_dir.startswith('job_'):
                full_dir_path = condor_wd + "/" + tmp_dir
                expected_condor_outDir = full_dir_path + "/outFiles"
                # Check if 'outFiles' dir exists
                if os.path.isdir(expected_condor_outDir):
                    _list_dir = os.listdir(expected_condor_outDir)
                    tmp_acc_full_path = ""
                    for file in _list_dir:
                        if file.endswith(".root"):
                            tmp_acc_full_path = expected_condor_outDir + "/" + file
                            break
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
                                    self.data_dirs.append(full_dir_path)
                                    self.data_files.append(tmp_acc_full_path)
                                else:
                                    # output ROOT file has been open but has not keys
                                    self.skipped_dirs.append(full_dir_path)
                                    self.skipped_file_noKeys += 1
                        else:
                            # output ROOT file has not been opened correctly
                            self.skipped_dirs.append(full_dir_path)
                            self.skipped_file_notReadable += 1
                    else:
                        # output ROOT file does not exist
                        self.skipped_dirs.append(full_dir_path)
                        self.skipped_file_notROOTfile += 1
                else:
                    # 'outFiles' dir does not exists
                    self.skipped_dirs.append(full_dir_path)
                    self.skipped_file_notFinalDir += 1

    def clean_condor_dir(self, dir):
        os.chdir(dir)
        outCondor = [filename for filename in os.listdir(
            '.') if filename.startswith("out")]

        # Clean the job dir
        if outCondor:
            for elm in outCondor:
                if os.path.isdir(elm):
                    shutil.rmtree(elm)
                if os.path.isfile(elm):
                    os.remove(elm)

    def resubmit_condor_jobs(self, skipped_dirs, verbose):
        for dir in skipped_dirs:
            self.clean_condor_dir(dir)

            # Submit HTCondor job
            if verbose:
                print('Resubmitting job from folder: {}'.format(dir))

            subprocess.run(
                "condor_submit -name sn-01.cr.cnaf.infn.it -spool crawler.sub", shell=True, check=True)

    def cargo(self):
        parser = ArgumentParser(
            description='CARGO Utility')
        parser.add_argument("-i", "--input", type=str,
                            dest='input', help='Input condor jobs WD')
        parser.add_argument("-o", "--output", type=str,
                            dest='output', help='Output ROOT files WD')
        args = parser.parse_args(sys.argv[2:])
        self.sub_opts = args

        for file in self.data_files:
            _idx = _mdir.rindex('/')
            _filename = file[_idx+1:]
            shutil.copy2(file, self.sub_opts.output + "/" + _filename)
        
        '''
        _dict = {}
        for _mdir in self.data_dirs:
            last_data_idx = _mdir.rindex('/')
            _name = _mdir[last_data_idx-6:last_data_idx]
            if _name not in _dict:
                _dict[_name] = []
            _dict[_name].append(_mdir)

        for _elm in _dict:
            _path = target_dir + "/" + _elm
            os.mkdir(_path)
            for _data in _dict[_elm]:
                _data_path = _data + "/outFiles"
                _idx_e = _data_path.rindex('/')
                _idx_s = _data_path[:_idx_e].rindex('/')
                _job = _data_path[_idx_s+1:_idx_e]
                _files = [_file for _file in os.listdir(
                    _data_path) if _file.endswith('.root')]
                for _file in _files:
                    _idx = _file.rindex('.')
                    _src = _data_path + "/" + _file
                    _dest = _path + "/" + f"{_file[:_idx]}_{_job}.root"
                    shutil.copy2(_src, _dest)
        '''

    def status(self):
        parser = ArgumentParser(
            description='DAMPE HTCondor Job Status Controller')
        parser.add_argument("-i", "--input", type=str,
                            dest='input', help='Input condor jobs WD')
        parser.add_argument("-r", "--resubmit", dest='resubmit', default=False,
                            action='store_true', help='HTCondor flag to resubmit failed jobs')
        parser.add_argument("-e", "--erase", dest='erase', default=False,
                            action='store_true', help='Remove ROOT files with no keys')
        parser.add_argument("-l", "--list", dest='list', default=False,
                            action='store_true', help='Produce file list')
        parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                            action='store_true', help='run in high verbosity mode')
        args = parser.parse_args(sys.argv[2:])
        self.sub_opts = args
        self.getListOfFiles(self.sub_opts.input)

        if self.sub_opts.verbose:
            print('Found {} GOOD condor directories'.format(len(self.data_dirs)))
        if self.skipped_dirs:
            print('Found {} BAD condor directories...\n'.format(
                len(self.skipped_dirs)))
            print('Found {} directories with no output folder'.format(
                self.skipped_file_notFinalDir))
            print('Found {} directories with inconsistent number of output ROOT files'.format(
                self.skipped_file_notAllOutput))
            print('Found {} directories with no output ROOT file'.format(
                self.skipped_file_notROOTfile))
            print('Found {} directories with corrupted output ROOT file'.format(
                self.skipped_file_notReadable))
            print('Found {} directories where output ROOT file has no keys\n'.format(
                self.skipped_file_noKeys))
            print('Here the folders list...\n')

            for idx, elm in enumerate(self.skipped_dirs):
                print('Skipped {} directory: {}'.format(idx, elm))

            if self.sub_opts.resubmit:
                print('\nResubmitting HTCondor jobs for {} directories\n'.format(
                    len(self.skipped_dirs)))
                for dir in self.skipped_dirs:
                    self.clean_condor_dir(dir)
                self.resubmit_condor_jobs(
                    self.skipped_dirs, self.sub_opts.verbose)
        if self.sub_opts.list:
            _list_path = self.sub_opts.input[self.sub_opts.input.rfind(
                '/')+1:] + ".txt"
            with open(_list_path, "w") as _final_list:
                for elm in self.data_files:
                    _final_list.write(elm + "\n")


if __name__ == '__main__':
    dampe_helper()

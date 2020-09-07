#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess
from argparse import ArgumentParser


class condor_task():

    def __init__(self):
        parser = ArgumentParser(
                description='Condor Deploy Helper',
                usage='''<command> [<args>]

	The most commonly used commands are:

		** DAMPE **
		dampe_flux     		call DAMPE all-electron flux facility
		dampe_acceptance 	call DAMPE acceptance facility
		dampe_mc		call DAMPE MC check facility

		** HERD **
		herd_acceptance		call HERD acceptance facility
		herd_anisotropy		call HERD anisotropy facility
	''')

        parser.add_argument('command', help='Subcommand to run')
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print("Unrecognized command")
            parser.print_help()
            exit(1)

        self.condorDirs = None
        self.sub_opts = None

        getattr(self, args.command)()

    def parse_input_list(self):
        self.condorDirs = []
        out_dir = self.sub_opts.directory
        with open(self.sub_opts.list, 'r') as inputList:
            list_idx = 0
            data_list = []
            for file_name in inputList:
                if not file_name.startswith('.'):
                    data_list.append(file_name.rstrip('\n'))
                    if len(data_list) == self.sub_opts.fileNumber:
                        _dir_data = self.write_list_to_file(
                            data_list, 
							out_dir, 
							list_idx)
                        if _dir_data[0]:
                            self.condorDirs.append(_dir_data[1])
                            list_idx += 1
                        data_list.clear()
            if data_list:
                _dir_data = self.write_list_to_file(
                    data_list, out_dir, list_idx)
                if _dir_data[0]:
                    self.condorDirs.append(_dir_data[1])
                    list_idx += 1
                data_list.clear()

    def get_list_path(self, out_dir, list_idx):
        final_list_path = out_dir + "/dataList_" + str(list_idx)
        return final_list_path

    def write_list_to_file(self, data_list, out_dir, list_idx):
        tmp_dir_name = out_dir + "/" + "job_" + str(list_idx)
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
            list_path = self.get_list_path(tmp_dir_name, list_idx) + ".txt"
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

    def create_condor_files(self, task):
        for idx, cDir in enumerate(self.condorDirs):

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
                    if task == "eFlux_acceptance":
                        self.acceptance_task(outScript, dataListPath, cDir)
                    elif task == "eFlux":
                        self.flux_task(outScript, dataListPath, cDir)
                    elif task == "MC_check":
                        self.mc_check_task(outScript, dataListPath, cDir)
                    else:
                        print("ERROR! Please, choose a correct task...")
                        sys.exit()

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

    def acceptance_task(self, outScript, dataListPath, cDir):
        tmpOutDir = cDir + str("/outFiles")
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
        outScript.write("dampe_init trunk\n")
        outScript.write('mkdir {}\n'.format(tmpOutDir))
        outScript.write('{} -a {} -d {} -w {} -v'.format(
            self.sub_opts.executable,
            dataListPath,
            tmpOutDir,
            self.sub_opts.config))

    def flux_task(self, outScript, dataListPath, cDir):
        tmpOutDir = cDir + str("/outFiles")
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
        outScript.write("dampe_init trunk\n")
        outScript.write('mkdir {}\n'.format(tmpOutDir))
        outScript.write('{} -i {} -d {} -w {} -g {} -b {} -t {} -v'.format(
            self.sub_opts.executable,
            dataListPath,
            tmpOutDir,
            self.sub_opts.config,
            self.sub_opts.geometry,
            self.sub_opts.background,
            self.sub_opts.livetime))

    def mc_check_task(self, outScript, dataListPath, cDir):
        tmpOutDir = cDir + str("/outFiles")
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
        outScript.write("dampe_init trunk\n")
        outScript.write('mkdir {}\n'.format(tmpOutDir))
        outScript.write('python {} -l {} -d {} -v'.format(
            self.sub_opts.executable,
            dataListPath,
            tmpOutDir))

    def dampe_flux(self):
        parser = ArgumentParser(description='DAMPE all-electron flux facility')
        parser.add_argument("-l", "--list", type=str,
                            dest='list', help='Input file list')
        parser.add_argument("-d", "--dir", type=str,
                            dest='directory', help='Target Directory')
        parser.add_argument("-i", "--iterate", type=str, dest='iterate',
                            help='Iterate through Target Directory - nTuples analysis')
        parser.add_argument("-x", "--executable", type=str,
                            dest='executable', help='Analysis script')
        parser.add_argument("-g", "--geometry", type=str,
                            dest='geometry', help='MC electron acceptance file')
        parser.add_argument("-b", "--background", type=str,
                            dest='background', help='MC proton background file')
        parser.add_argument("-e", "--livetime", type=str,
                            dest='livetime', help='DAMPE acqusition livetime')
        parser.add_argument("-c", "--config", type=str,
                            dest='config', help='Software Config Directory')
        parser.add_argument("-n", "--number", type=int, dest='fileNumber',
                            const=100, nargs='?', help='number of files per job')
        parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                            action='store_true', help='run in high verbosity mode')
        parser.add_argument("-r", "--recreate", dest='recreate', default=False,
                            action='store_true', help='recreate output dirs if present')
        args = parser.parse_args(sys.argv[2:])
        self.sub_opts = args

        if self.sub_opts.list:
            self.parse_input_list()
            self.create_condor_files(task="eFlux")
            self.submit_jobs()
        if self.sub_opts.iterate and (not self.sub_opts.list) and (not self.sub_opts.directory):
            nTuples_wdcontent = [self.sub_opts.iterate + "/" +
                                 tmp_dir for tmp_dir in os.listdir(self.sub_opts.iterate)]
            for dir in nTuples_wdcontent:
                if "201" in dir:
                    self.sub_opts.directory = dir
                    self.sub_opts.list = dir + "/dataFileList.txt"
                    self.parse_input_list()
                    self.create_condor_files(task="eFlux")
                    self.submit_jobs()

    def dampe_acceptance(self):
        parser = ArgumentParser(description='DAMPE all-electron flux facility')
        parser.add_argument("-l", "--list", type=str,
                            dest='list', help='Input file list')
        parser.add_argument("-d", "--dir", type=str,
                            dest='directory', help='Target Directory')
        parser.add_argument("-x", "--executable", type=str,
                            dest='executable', help='Analysis script')
        parser.add_argument("-c", "--config", type=str,
                            dest='config', help='Software Config Directory')
        parser.add_argument("-n", "--number", type=int, dest='fileNumber',
                            const=100, nargs='?', help='number of files per job')
        parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                            action='store_true', help='run in high verbosity mode')
        parser.add_argument("-r", "--recreate", dest='recreate', default=False,
                            action='store_true', help='recreate output dirs if present')
        args = parser.parse_args(sys.argv[2:])
        self.sub_opts = args

        if self.sub_opts.list:
            self.parse_input_list()
            self.create_condor_files(task="eFlux_acceptance")
            self.submit_jobs()

    def dampe_mc(self):
        parser = ArgumentParser(description='DAMPE all-electron flux facility')
        parser.add_argument("-l", "--list", type=str,
                            dest='list', help='Input file list')
        parser.add_argument("-d", "--dir", type=str,
                            dest='directory', help='Target Directory')
        parser.add_argument("-x", "--executable", type=str,
                            dest='executable', help='Analysis script')
        parser.add_argument("-n", "--number", type=int, dest='fileNumber',
                            const=100, nargs='?', help='number of files per job')
        parser.add_argument("-v", "--verbose", dest='verbose', default=False,
                            action='store_true', help='run in high verbosity mode')
        parser.add_argument("-r", "--recreate", dest='recreate', default=False,
                            action='store_true', help='recreate output dirs if present')
        args = parser.parse_args(sys.argv[2:])
        self.sub_opts = args

        if self.sub_opts.list:
            self.parse_input_list()
            self.create_condor_files(task="MC_check")
            self.submit_jobs()


if __name__ == '__main__':
    condor_task()
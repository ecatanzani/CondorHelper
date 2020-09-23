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
		dampe_flux_collector    call DAMPE all-electron flux facility
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

        self.condorDirs = []
        self.sub_opts = None
        self.years_content = {}

        getattr(self, args.command)()

    def parse_input_list(self):
        out_dir = self.sub_opts.output
        with open(self.sub_opts.list, 'r') as inputList:
            list_idx = 0
            data_list = []
            for file_name in inputList:
                if not file_name.startswith('.'):
                    data_list.append(file_name.rstrip('\n'))
                    if len(data_list) == self.sub_opts.depth:
                        _tmp_out_dir = out_dir + "/" + "job_" + str(list_idx)
                        _dir_data = self.write_list_to_file(data_list, _tmp_out_dir)
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
                    year = filename[year_fidx : year_fidx + 4]
                    month = filename[year_fidx + 4 : year_fidx + 6]
                    if year not in self.years_content:
                        self.years_content[year] = {}
                    if month not in self.years_content[year]:
                        self.years_content[year][month] = []

                    self.years_content[year][month].append(filename.rstrip('\n'))

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
                        _dir_data = self.write_list_to_file(data_list, _tmp_out_dir)
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

    def create_rti_tree(self, current_dir, input_list, verbose):
        import csv
        import numpy as np
        from ROOT import TFile, TTree, TMath

        # Create glon e glat variables
        glat = np.empty((1), dtype="float")
        glon = np.empty((1), dtype="float")
        thetas = np.empty((1), dtype="float")
        phi = np.empty((1), dtype="float")
        events = np.empty((1), dtype="int")
        geo_lat = np.empty((1), dtype="float")
        geo_lon = np.empty((1), dtype="float")

        # Create new TFile
        outfile = current_dir + "/RTI_tree.root"
        RTI_output_file = TFile(outfile, "RECREATE")
        if RTI_output_file.IsZombie():
            print('Error writing output TFile [{}]'.format(outfile))
            sys.exit()

        # Create final Tree
        RTI_tree = TTree("RTI_tree", "AMS RTI TTree")

        # Branch the TTree
        RTI_tree.Branch("glat", glat, "glat/D")
        RTI_tree.Branch("glon", glon, "glon/D")
        RTI_tree.Branch("thetas", thetas, "thetas/D")
        RTI_tree.Branch("phi", phi, "phi/D")
        RTI_tree.Branch("events", events, "events/I")
        RTI_tree.Branch("geo_lat", geo_lat, "geo_lat/D")
        RTI_tree.Branch("geo_lon", geo_lon, "geo_lon/D")
        
        # Parse input list
        with open(outfile, 'r') as input_list:
            csv_files = input_list.readlines()
            for file in csv_files:
                with open(file.rstrip("\n"), 'r') as tmp_file:
                    csv_reader = csv.reader(tmp_file, delimiter=' ', quoting=csv.QUOTE_NONE)
                    line_count = 0
                    for row in csv_reader:
                        if line_count != 0:
                            if int(row[1]):
                                thetas[0] = float(row[18])
                                phi[0] = float(row[19])
                                glat[0] = float(row[22])
                                glon[0] = float(row[23])
                                events[0] = int(row[24])
                                geo_lat[0] = thetas*TMath.RadToDeg()
                                geo_lon[0] = phi*TMath.RadToDeg()
                                RTI_tree.Fill()
                        line_count += 1
                    if verbose:
                        print('Has been read {} lines [{}]'.format(line_count, file.rstrip("\n")))
        RTI_tree.Write()
        RTI_output_file.Close()

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

    def create_condor_files(self, task):
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
                    if task == "eflux_acceptance":
                        self.acceptance_task(outScript, dataListPath, cDir)
                    elif task == "eflux_collector":
                        self.flux_task(outScript, dataListPath, cDir)
                    elif task == "eflux_ntuples":
                        self.flux_ntuple_task(outScript, dataListPath, cDir)
                    elif task == "MC_check":
                        self.mc_check_task(outScript, dataListPath, cDir)
                    elif task == "HERD_anisotropy":
                        self.create_rti_tree(cDir, dataListPath, self.sub_opts.verbose)

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
        outScript.write('{} -w {} -i {} -d {} -a -v'.format(
            self.sub_opts.executable,
            self.sub_opts.config,
            dataListPath,
            tmpOutDir))

    def flux_task(self, outScript, dataListPath, cDir):
        tmpOutDir = cDir + str("/outFiles")
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
        outScript.write("dampe_init trunk\n")
        outScript.write('mkdir {}\n'.format(tmpOutDir))
        outScript.write('{} -w {} -i {} -d {} -r -v'.format(
            self.sub_opts.executable,
            self.sub_opts.config,
            dataListPath,
            tmpOutDir))

    def flux_ntuple_task(self, outScript, dataListPath, cDir):
        tmpOutDir = cDir + str("/outFiles")
        outScript.write("#!/usr/bin/env bash\n")
        outScript.write("source /cvmfs/dampe.cern.ch/centos7/etc/setup.sh\n")
        outScript.write("dampe_init trunk\n")
        outScript.write('mkdir {}\n'.format(tmpOutDir))
        outScript.write('{} -w {} -i {} -d {} -n -v'.format(
            self.sub_opts.executable,
            self.sub_opts.config,
            dataListPath,
            tmpOutDir))

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
    
    def herd_anisotropy_task(self, outScript, dataListPath, cDir):
        tmpOutDir = cDir + str("/outFiles")
        outScript.write("#!/usr/bin/env bash\n")
        

    def dampe_flux_collector(self):
        parser = ArgumentParser(description='DAMPE all-electron flux collector facility')
        parser.add_argument("-l", "--list", type=str, dest='list', help='Input DATA/MC list')
        parser.add_argument("-o", "--output", type=str, dest='output', help='HTC output directory')
        parser.add_argument("-n", "--ntuple", dest='ntuple', default=False, action='store_true', help='nTuple facility')
        parser.add_argument("-d", "--depth", type=int, dest='depth', const=100, nargs='?', help='files to process in job')
        parser.add_argument("-x", "--executable", type=str, dest='executable', help='Analysis script')
        parser.add_argument("-c", "--config", type=str, dest='config', help='Software Config Directory')
        parser.add_argument("-v", "--verbose", dest='verbose', default=False, action='store_true', help='run in high verbosity mode')
        parser.add_argument("-r", "--recreate", dest='recreate', default=False, action='store_true', help='recreate output dirs if present')
        args = parser.parse_args(sys.argv[2:])
        self.sub_opts = args
        
        if self.sub_opts.ntuple:
            self.extract_timing_info()
            self.parse_timing_info()
            self.create_condor_files(task="eflux_ntuples")  
        else:
            self.parse_input_list()
            self.create_condor_files(task="eflux_collector")
        self.submit_jobs()

    def dampe_acceptance(self):
        parser = ArgumentParser(description='DAMPE acceptance facility')
        parser.add_argument("-l", "--list", type=str, dest='list', help='Input file list')
        parser.add_argument("-o", "--output", type=str, dest='output', help='HTC output directory')
        parser.add_argument("-d", "--depth", type=int, dest='depth', const=100, nargs='?', help='files to process in job')
        parser.add_argument("-x", "--executable", type=str, dest='executable', help='Analysis script')
        parser.add_argument("-c", "--config", type=str, dest='config', help='Software Config Directory')
        parser.add_argument("-v", "--verbose", dest='verbose', default=False, action='store_true', help='run in high verbosity mode')
        parser.add_argument("-r", "--recreate", dest='recreate', default=False, action='store_true', help='recreate output dirs if present')
        args = parser.parse_args(sys.argv[2:])
        self.sub_opts = args

        if self.sub_opts.list:
            self.parse_input_list()
            self.create_condor_files(task="eflux_acceptance")
            self.submit_jobs()

    def dampe_mc(self):
        parser = ArgumentParser(description='DAMPE MC check facility')
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

    def herd_anisotropy(self):
        parser = ArgumentParser(description='HERD anisotropy facility')
        parser.add_argument("-l", "--list", type=str,
                            dest='list', help='Input file list')
        parser.add_argument("-d", "--dir", type=str,
                            dest='directory', help='Target Directory')
        parser.add_argument("-x", "--executable", type=str,
                            dest='executable', help='Analysis script')
        parser.add_argument("-a", "--acceptance", type=str,
                            dest='acceptance', help='Input acceptance TFile')
        parser.add_argument("-e", "--event", type=str,
                            dest='event', help='Input angular event distribution TFile')
        parser.add_argument("-f", "--flux", type=str,
                            dest='flux', help='Input flux TFile')
        parser.add_argument("-t", "--telemetry", type=str,
                            dest='telemetry', help='Input telemetry TFile')
        parser.add_argument("-z", "--hz", type=str,
                            dest='hz', help='Input DAMPE acquisition rate TFile')
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
            self.create_condor_files(task="HERD_anisotropy")
            self.submit_jobs()


if __name__ == '__main__':
    condor_task()
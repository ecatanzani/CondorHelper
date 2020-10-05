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
		dampe_status		call DAMPE HTCondor status facility

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
		self.skipped_dirs = []
		self.data_dirs = []
		self.skipped_file_notFinalDir = 0
		self.skipped_file_notAllOutput = 0
		self.skipped_file_noSingleROOTfile = 0
		self.skipped_file_notROOTfile = 0
		self.skipped_file_notReadable = 0
		self.skipped_file_noKeys = 0
		self.dampe_ntuples_file_size = 6

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
					csv_reader = csv.reader(
						tmp_file, delimiter=' ', quoting=csv.QUOTE_NONE)
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
						print('Has been read {} lines [{}]'.format(
							line_count, file.rstrip("\n")))
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
						self.create_rti_tree(
							cDir, dataListPath, self.sub_opts.verbose)

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
		parser = ArgumentParser(
			description='DAMPE all-electron flux collector facility')
		parser.add_argument("-l", "--list", type=str,
							dest='list', help='Input DATA/MC list')
		parser.add_argument("-o", "--output", type=str,
							dest='output', help='HTC output directory')
		parser.add_argument("-n", "--ntuple", dest='ntuple',
							default=False, action='store_true', help='nTuple facility')
		parser.add_argument("-d", "--depth", type=int, dest='depth',
							const=100, nargs='?', help='files to process in job')
		parser.add_argument("-x", "--executable", type=str,
							dest='executable', help='Analysis script')
		parser.add_argument("-c", "--config", type=str,
							dest='config', help='Software Config Directory')
		parser.add_argument("-v", "--verbose", dest='verbose', default=False,
							action='store_true', help='run in high verbosity mode')
		parser.add_argument("-r", "--recreate", dest='recreate', default=False,
							action='store_true', help='recreate output dirs if present')
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
		parser.add_argument("-l", "--list", type=str,
							dest='list', help='Input file list')
		parser.add_argument("-o", "--output", type=str,
							dest='output', help='HTC output directory')
		parser.add_argument("-d", "--depth", type=int, dest='depth',
							const=100, nargs='?', help='files to process in job')
		parser.add_argument("-x", "--executable", type=str,
							dest='executable', help='Analysis script')
		parser.add_argument("-c", "--config", type=str,
							dest='config', help='Software Config Directory')
		parser.add_argument("-v", "--verbose", dest='verbose', default=False,
							action='store_true', help='run in high verbosity mode')
		parser.add_argument("-r", "--recreate", dest='recreate', default=False,
							action='store_true', help='recreate output dirs if present')
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

	def dampe_getListOfFiles(self, condor_wd, ntuple_flag=False):
		from ROOT import TFile

		if not ntuple_flag:
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
							tmp_acc_file = TFile.Open(
								tmp_acc_full_path, "READ")
							# Check if output ROOT file is redable
							if tmp_acc_file.IsOpen():
								# Check if output ROOT file has keys
								outKeys = tmp_acc_file.GetNkeys()
								if outKeys:
									self.data_dirs.append(full_dir_path)
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
		else:
			# Starting loop on output condor dirs
			for tmp_dir in os.listdir(condor_wd):
				_dir_path = condor_wd + "/" + tmp_dir
				for jobs_dir in os.listdir(_dir_path):
					if jobs_dir.startswith('job_'):
						full_dir_path = _dir_path 
						full_dir_path += "/" + jobs_dir
						expected_condor_outDir = full_dir_path + "/outFiles"
						# Check if 'outFiles' dir exists
						if os.path.isdir(expected_condor_outDir):
							files_in_dir = [
								_file for _file in os.listdir(expected_condor_outDir) if _file.endswith(".root")]
							tmp_acc_full_path = [
								expected_condor_outDir + "/" + _file for _file in files_in_dir]
							# Check if the output file number is consistent
							if (len(tmp_acc_full_path) == self.dampe_ntuples_file_size):
								# Check if output ROOT files exists
								_good_file = [False]*len(tmp_acc_full_path)
								for elm_idx, elm in enumerate(tmp_acc_full_path):
									if os.path.isfile(elm):
										tmp_acc_file = TFile.Open(elm, "READ")
										# Check if output ROOT file is readable
										if tmp_acc_file.IsOpen():
											# Check if output ROOT file has keys
											outKeys = tmp_acc_file.GetNkeys()
											if outKeys:
												_good_file[elm_idx] = True
											else:
												# output ROOT file has been open but has not keys
												self.skipped_dirs.append(full_dir_path)
												self.skipped_file_noKeys += 1
												break
										else:
											# output ROOT file has not been opened correctly
											self.skipped_dirs.append(full_dir_path)
											self.skipped_file_notReadable += 1
											break
									else:
										# output ROOT file does not exist
										self.skipped_dirs.append(full_dir_path)
										self.skipped_file_noSingleROOTfile += 1
										break
								if all(_good_file):
									self.data_dirs.append(full_dir_path)
							elif not tmp_acc_full_path:
								# output ROOT file does not exist
								self.skipped_dirs.append(full_dir_path)
								self.skipped_file_notROOTfile += 1
							else:	
								# not consistent number of output files
								self.skipped_dirs.append(full_dir_path)
								self.skipped_file_notAllOutput += 1
						else:
							# 'outFiles' dir does not exists
							self.skipped_dirs.append(full_dir_path)
							self.skipped_file_notFinalDir += 1
	
	def clean_condor_dir(self, dir):
		os.chdir(dir)
		outCondor = [filename for filename in os.listdir('.') if filename.startswith("out")]
			
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

			subprocess.run("condor_submit -name sn-01.cr.cnaf.infn.it -spool crawler.sub", shell=True, check=True)
	
	def cargo(self, target_dir):
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
				_files = [_file for _file in os.listdir(_data_path) if _file.endswith('.root')]
				for _file in _files:
					_idx = _file.rindex('.')
					_src = _data_path + "/" + _file
					_dest = _path + "/" + f"{_file[:_idx]}_{_job}.root"
					shutil.copy2(_src, _dest)
		

	def dampe_status(self):
		parser = ArgumentParser(
			description='DAMPE HTCondor Job Status Controller')
		parser.add_argument("-i", "--input", type=str,
							dest='input', help='Input condor jobs WD')
		parser.add_argument("-n", "--ntuple", dest='ntuple',
							default=False, action='store_true', help='ntuple data dype')
		parser.add_argument("-r", "--resubmit", dest='resubmit', default=False,
                        action='store_true', help='HTCondor flag to resubmit failed jobs')
		parser.add_argument("-e", "--erase", dest='erase', default=False,
                        action='store_true', help='Remove ROOT files with no keys')
		parser.add_argument("-m", "--move", type=str,
							dest='move', help='Move ROOT nTuples to target directory')
		parser.add_argument("-v", "--verbose", dest='verbose', default=False,
						action='store_true', help='run in high verbosity mode')
		args = parser.parse_args(sys.argv[2:])
		self.sub_opts = args
		
		self.dampe_getListOfFiles(self.sub_opts.input, self.sub_opts.ntuple)

		if self.sub_opts.verbose:
			print('Found {} GOOD condor directories'.format(len(self.data_dirs)))
		if self.skipped_dirs:
			print('Found {} BAD condor directories...\n'.format(len(self.skipped_dirs)))
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
				self.resubmit_condor_jobs(self.skipped_dirs, self.sub_opts.verbose)
		if self.sub_opts.move:
			self.cargo(self.sub_opts.move)

if __name__ == '__main__':
	condor_task()

import pyabf
import pandas as pd
import numpy as np
import sys
import time
import csv
import os
import multiprocessing

start = time.time()

# channel settings [0: x, 2: z, 4: ioncurrent]
abf_channel_list = [0, 2, 4]

# multithreading counts
thread_num = 8

args_num = len(sys.argv)

if args_num <= 1:
	print('usage: ')
	print('python abfexx.py [file1.abf] [file2.abf] ...')
	print('python abfexx.py -r [directory1] [directory2] ...')
	exit()

args_list = sys.argv

def DirWalkThrough(base_dir_path):
	file_path_list = []
	for iter in os.walk(base_dir_path):
		for jter in iter[2]:
			if jter.endswith('.abf') or jter.endswith('.ABF'):
				filePath = ''
				if iter[0].endswith('/'):
					filePath = iter[0] + jter
				else:
					file_path = iter[0] + '/' + jter
				file_path_list.append(filePath)
	return file_path_list

# locate each abf file
file_path_list = []
if args_list[1] == '-r':
	for iter in range(2, args_num):
		file_path_list += DirWalkThrough(sys.argv[iter])
else:
	for iter in range(1, args_num):
		file_path_list.append(sys.argv[iter])

# allocate multithread tasks to cores
file_num = len(file_path_list)
file_multiples = file_num // thread_num
file_remainder = file_num % thread_num
actual_core_counts = thread_num if file_multiples > 0 else file_remainder
core_segments = np.array(np.ones((thread_num))).astype(int)
core_segments = (core_segments * file_multiples + np.concatenate((np.ones((file_remainder)), np.zeros((thread_num-file_remainder))))).astype(int)
for iter in range(1, actual_core_counts):
	core_segments[iter] += core_segments[iter-1]

process_indicator = multiprocessing.Value('i', 0)

# thread process function
def ThreadProc(start_index, end_index):
	global process_indicator
	for iter in range(start_index, end_index):
		abf = pyabf.ABF(file_path_list[iter])
		coarray_list = []
		for jter in range(len(abf_channel_list)):
			abf.setSweep(sweepNumber=0, channel=abf_channel_list[jter])
			coarray_list.append(np.array([abf.sweepY]))
		coarray = np.concatenate(coarray_list, axis=0).T
		# csv writer
		csvfilename = os.path.splitext(file_path_list[iter])[0] + '.csv'
		with open(csvfilename, mode='w') as hcsvfp:
			csvwriter = csv.writer(hcsvfp, delimiter=',')
			csvwriter.writerows(coarray)
			hcsvfp.close()
			process_indicator.value += 1
			print('%s conversion done. (%d/%d)' % (csvfilename, process_indicator.value, file_num))
			sys.stdout.flush()
		
def paralleling_operation():
	hThreads = []
	for iter in range(actual_core_counts):
		hThreads.append(multiprocessing.Process(target = ThreadProc, args=(core_segments[iter-1] if iter > 0 else int(0), core_segments[iter])))
		hThreads[iter].start()
	# wait for multiple termination events
	for iter in range(actual_core_counts):
		hThreads[iter].join()

paralleling_operation()

end = time.time()
print('ABFExtraction multi-thread, %d file(s) converted, %f s elapsed.' % (file_num, end-start))



















import pyabf
import pandas as pd
import numpy as np
import os, sys, time, json
import multiprocessing

def DirWalkThrough(base_dir_path):
	file_path_list = []
	for iter in os.walk(base_dir_path):
		for jter in iter[2]:
			if jter.endswith('.abf') or jter.endswith('.ABF'):
				filePath = ''
				if iter[0].endswith('/'):
					filePath = iter[0] + jter
				else:
					filePath = iter[0] + '/' + jter
				file_path_list.append(filePath)
	return file_path_list

# thread process function
def ThreadProc(abf_channel_list, abf_sweep_list, extract_all_sweep, file_path_list, start_index, end_index, process_indicator):
    for iter in range(start_index, end_index):
        abf = pyabf.ABF(file_path_list[iter])
        if extract_all_sweep:
            sweep_list = [sweep_idx for sweep_idx in range(abf.sweepCount)]
        else:
            sweep_list = abf_sweep_list
        # json info dict
        info_dict = {}
        info_dict['sampling_rate'] = abf.sampleRate
        info_dict['abf_channels'] = abf_channel_list
        info_dict['abf_sweeps'] = sweep_list
        info_dict['abf_epoch_p1s'] = abf.sweepEpochs.p1s
        info_dict['abf_epoch_p2s'] = abf.sweepEpochs.p2s
        coarray_list = []
        for jter in range(len(abf_channel_list)):
            for kter in range(len(sweep_list)):
                abf.setSweep(sweepNumber=sweep_list[kter], channel=abf_channel_list[jter])
                coarray_list.append(np.array([abf.sweepY])) 
        coarray = np.concatenate(coarray_list, axis=0).T
		# csv save
        csvfilename = os.path.splitext(file_path_list[iter])[0] + '.csv'
        df = pd.DataFrame(coarray)
        df.to_csv(csvfilename, index=False, header=None)
        # json save
        jsonfilename = os.path.splitext(file_path_list[iter])[0] + '.json'
        with open(jsonfilename, 'w') as jsonfh:
            json.dump(info_dict, jsonfh)
            jsonfh.close()
        with process_indicator.get_lock():
            process_indicator.value += 1
            tmp_iter_value = process_indicator.value
        print('"%s" conversion done. (%d/%d)' % (csvfilename, tmp_iter_value, len(file_path_list)))
        sys.stdout.flush()
		
def paralleling_operation(abf_channel_list, abf_sweep_list, extract_all_sweep, file_path_list, process_indicator):
    hThreads = []
    for iter in range(actual_core_counts):
    	hThreads.append(multiprocessing.Process(target = ThreadProc, args=(abf_channel_list, abf_sweep_list, extract_all_sweep, file_path_list, core_segments[iter-1] if iter > 0 else int(0), core_segments[iter], \
            process_indicator)))
    	hThreads[iter].start()
    # wait for multiple termination events    
    for iter in range(actual_core_counts):
        hThreads[iter].join()

if __name__ == '__main__':
    # /////////////////////////////////////// Only Parameters You Need to Change ///////////////////////////////////////
	# channel settings, e.g. [0: x, 2: z, 4: ioncurrent]
    abf_channel_list = [0, 1]
    # sweeps (if "extract_all_sweep" is true, "abf_sweep_list" will be ignored) 
    extract_all_sweep = True
    abf_sweep_list = [0, 1, 2]
    # //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # global counter
    process_indicator = multiprocessing.Value('i', 0)
	# multithreading counts (set to host actual core counts)
    thread_num = multiprocessing.cpu_count()
    args_num = len(sys.argv)
    start = time.time()
    if args_num <= 1:
        print('usage: ')
        print('python abfexx.py [file1.abf] [file2.abf] ...')
        print('python abfexx.py -r [directory1] [directory2] ...')
        sys.exit()  
    args_list = sys.argv
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
    paralleling_operation(abf_channel_list, abf_sweep_list, extract_all_sweep, file_path_list, process_indicator)
    end = time.time()
    print('ABFExtraction multi-thread, %d file(s) converted, %f s elapsed.' % (file_num, end-start))
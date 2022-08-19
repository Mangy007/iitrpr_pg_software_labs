import os
import sys
from block import Block
from memory import Memory
from utility import Utility
import time

block_size, memory_size = input().split()

block_size, memory_size = [int(block_size), int(memory_size)]

if memory_size < 3:
    sys.exit("Incorrect input")

start_time = time.time()

path = os.getcwd()
data_dir_path = path+"/dataset"
data_filename = "/data.txt"
unsorted_data_block_dir_path = path+"/data_block"
run_dir_path = path+"/sorted_run"
initial_run_dir_path = run_dir_path+"/initial_run"
intermediate_run_dir_path = run_dir_path+"/intermediate_run"

Utility.create_directories(unsorted_data_block_dir_path)

records = open(data_dir_path+data_filename).readlines()
block_name = Utility.create_disk_blocks(block_size, records, unsorted_data_block_dir_path, "US_")
Utility.reinitialize_block_number()

counter = 0
memory = None
Utility.create_directories(initial_run_dir_path)
blocks_pointers = open(initial_run_dir_path+"/R_pointers.txt","w")

while block_name != 'NULL':
    if counter % memory_size == 0:
        memory = Memory(block_size, memory_size)
    unsorted_data_block = open(unsorted_data_block_dir_path+"/"+block_name+".txt").readlines()
    block = Block(unsorted_data_block[:-1], unsorted_data_block[-1], block_size)
    memory.add_block(block)
    if memory.size() == memory_size:
        Utility.create_blocks_starting_pointers(blocks_pointers)
        memory.create_initial_runs(initial_run_dir_path, block_size)
    block_name = unsorted_data_block[-1]
    counter += 1

if memory.size() > 0:
    Utility.create_blocks_starting_pointers(blocks_pointers)
    memory.create_initial_runs(initial_run_dir_path, block_size)

Utility.reinitialize_block_number()
# total_runs = open(initial_run_dir_path+"/R_pointers.txt", "r")
blocks_pointers.close()

total_runs = Utility.get_runs_count(initial_run_dir_path+"/R_pointers.txt")
counter = 1
intermediate_run_parent_dir_path = intermediate_run_dir_path

while total_runs > 1:
    memory = Memory(block_size, memory_size)
    # print("total runs: ", total_runs)
    new_intermediate_run_dir_path = memory.merge_runs(initial_run_dir_path, intermediate_run_dir_path, counter)
    initial_run_dir_path = intermediate_run_dir_path+"_"+str(counter)
    intermediate_run_dir_path = intermediate_run_parent_dir_path
    total_runs = Utility.get_runs_count(intermediate_run_dir_path+"_"+str(counter)+"/R_pointers.txt")
    counter += 1

Utility.generate_final_sorted_data(run_dir_path, intermediate_run_dir_path+"_"+str(counter-1))

end_time = time.time()

print("total time taken in secs: ",end_time-start_time,)






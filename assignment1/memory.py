from heapq import heappop, heappush
from block import Block
from utility import Utility

class Memory:

    def __init__(self, block_size:int, memory_size:int):
        self.memory_size = memory_size
        self.block_size = block_size
        self.memory_blocks = []
        self.runs = []
        self.output_block = None

    def add_block(self, block: Block):
        self.memory_blocks.append(block)

    def size(self) -> int:
        return len(self.memory_blocks)

    def create_initial_runs(self, runs_dir_path: str, block_size: int):
        # sort the data/records based on transaction sale amount where 
        # each record is a tuple (transaction_id, sale_amount, cust_name, item_category)
        complete_records = []
        for memory_block in self.memory_blocks:
            for record in memory_block.records:
                complete_records.append(record)
        complete_records.sort(key = lambda sale_amount : sale_amount[1])
        Utility.create_directories(runs_dir_path)
        Utility.create_disk_blocks(block_size, complete_records, runs_dir_path, "B_")
        self.memory_blocks.clear()

    def sort_runs_blocks(self, prev_runs_dir_path, runs_dir_path, intermediate_runs_blocks_pointers):
        min_heap = []
        is_last_block = False
        is_run_first_output_block = False

        # print(self.memory_blocks)

        while len(self.memory_blocks)>0:
            for block in self.memory_blocks:
                # index = block.current_block_record_index
                # if index >= block.size():
                #     if block.next_block != 'NULL':
                #         data = open(prev_runs_dir_path+'/'+block.next_block+'.txt').readlines()
                #         self.memory_blocks.remove(block)
                #         block = Block(data[:-1], data[-1], self.block_size)
                #         self.memory_blocks.append(block)
                #         index = block.current_block_record_index
                #     else:
                #         self.memory_blocks.remove(block)
                #         # is_last_block = True
                #         continue
                is_block_record_present_in_heap = False
                for node in min_heap:
                    if block == node[2]:
                        is_block_record_present_in_heap = True
                # add record to min heap if record not present in heap
                if not is_block_record_present_in_heap:
                    heappush(min_heap, (block.records[block.current_block_record_index][1], block.records[block.current_block_record_index][0], block))
            # print("min heap ",min_heap)
            # if len(min_heap) == 0:#if len(self.memory_blocks)==0:
            #     print("aaya")
            #     is_last_block = True
            #     break
            min_record_block = heappop(min_heap)[2]
            # print("records ",min_record_block.records)
            # index = min_record_block.current_block_record_index
            if self.output_block is None:
                self.output_block = Block([],"", self.block_size)
                self.output_block.add_record(min_record_block.records[min_record_block.current_block_record_index])
                min_record_block.current_block_record_index += 1
            else:
                self.output_block.add_record(min_record_block.records[min_record_block.current_block_record_index])
                min_record_block.current_block_record_index += 1

            index = min_record_block.current_block_record_index
            if index >= min_record_block.size():
                if min_record_block.next_block != 'NULL':
                    data = open(prev_runs_dir_path+'/'+min_record_block.next_block+'.txt').readlines()
                    self.memory_blocks.remove(min_record_block)
                    block = Block(data[:-1], data[-1], self.block_size)
                    self.memory_blocks.append(block)
                    index = block.current_block_record_index
                else:
                    self.memory_blocks.remove(min_record_block)
                    # is_last_block = True
            
            if len(self.memory_blocks)==0:
                # print("aaya")
                is_last_block = True

            # print(self.output_block.size(), self.block_size, len(self.memory_blocks), self.output_block.records, index, is_last_block, len(min_heap))
            if self.output_block.size() == self.block_size:
                if is_last_block:
                    if not is_run_first_output_block:
                        Utility.create_blocks_starting_pointers(intermediate_runs_blocks_pointers)
                        is_run_first_output_block = True
                    Utility.create_single_disk_block(self.block_size, self.output_block.records, runs_dir_path, "B_", is_last_block)
                    self.output_block = None
                    is_last_block = False
                else:
                    if not is_run_first_output_block:
                        Utility.create_blocks_starting_pointers(intermediate_runs_blocks_pointers)
                        is_run_first_output_block = True
                    Utility.create_single_disk_block(self.block_size, self.output_block.records, runs_dir_path, "B_", is_last_block)
                    self.output_block = None
            elif is_last_block:
                if not is_run_first_output_block:
                    Utility.create_blocks_starting_pointers(intermediate_runs_blocks_pointers)
                    is_run_first_output_block = True
                Utility.create_single_disk_block(self.block_size, self.output_block.records, runs_dir_path, "B_", is_last_block)
                self.output_block = None
                is_last_block = False
        
        # Utility.reinitialize_block_number()

    def merge_runs(self, runs_dir_path: str, intermediate_runs_dir_path: str, intermediate_runs_counter: int):
        runs_block_pointers = open(runs_dir_path+"/R_pointers.txt", 'r')
        intermediate_runs_dir = intermediate_runs_dir_path+"_"+str(intermediate_runs_counter)
        Utility.create_directories(intermediate_runs_dir)
        self.runs = [run.strip() for run in runs_block_pointers.readlines()]
        intermediate_runs_blocks_pointers = open(intermediate_runs_dir+"/R_pointers.txt","w")
        # print(self.runs)
        i = 0
        # when (#runs > M-1)
        while i < len(self.runs):
            # number of blocks in memory == memory_size-1 as one memory block reserved for output block
            if self.size() == (self.memory_size - 1):
                self.sort_runs_blocks(runs_dir_path, intermediate_runs_dir, intermediate_runs_blocks_pointers)
                self.memory_blocks.clear()
            data_block = open(runs_dir_path+'/'+self.runs[i]+'.txt').readlines()
            # print(data_block[:-1])
            block = Block(data_block[:-1], data_block[-1], self.block_size)
            self.add_block(block)
            i += 1
        
        # print("runs left ",self.size())
        # when (#runs <= M-1)
        if self.size() > 0:
            self.sort_runs_blocks(runs_dir_path, intermediate_runs_dir, intermediate_runs_blocks_pointers)
            self.memory_blocks.clear()
        Utility.reinitialize_block_number()
import os

block_number = 1

class Utility:

    @staticmethod
    def reinitialize_block_number():
        global block_number
        block_number = 1

    @staticmethod
    def create_directories(dir_path):
        try:
        # create a new directory
            os.makedirs(dir_path)
        except OSError:
        # directory is already present
            pass
    
    @staticmethod
    # create disk blocks based on block size
    def create_disk_blocks(block_size: int, data: list, data_block_dir_path: str, file_prefix: str) -> str:
        global block_number
        data_block = open(data_block_dir_path+"/"+file_prefix+str(block_number)+".txt","w")
        initial_block_name = file_prefix+str(block_number)

        for i in range(0,len(data)):
            data_block.write(str(data[i]).strip('\n'))
            data_block.write('\n')
            if i == len(data)-1:
                block_number += 1
                data_block.write("NULL")
                data_block.close()
            elif (i+1) % block_size == 0:
                block_number += 1
                data_block.write(file_prefix+str(block_number))
                data_block.close()
                data_block = open(data_block_dir_path+"/"+file_prefix+str(block_number)+".txt","w")
        
        return initial_block_name

    @staticmethod
    # create disk blocks based on block size
    def create_single_disk_block(block_size: int, data: list, data_block_dir_path: str, file_prefix: str, is_last_block: bool) -> str:
        global block_number
        data_block = open(data_block_dir_path+"/"+file_prefix+str(block_number)+".txt","w")
        initial_block_name = file_prefix+str(block_number)

        for i in range(0,len(data)):
            data_block.write(str(data[i]).strip('\n'))
            data_block.write('\n')
            if i == len(data)-1:
                block_number += 1
                if is_last_block:
                    data_block.write("NULL")
                else:
                    data_block.write(file_prefix+str(block_number))
                data_block.close()
        
        return initial_block_name

    @staticmethod
    def create_blocks_starting_pointers(blocks_pointers):
        blocks_pointers.write('B_'+str(block_number)+"\n")

    @staticmethod
    def get_runs_count(file_path:str) -> int:
        file = open(file_path, 'r')
        runs = file.readlines()
        file.close()
        return len(runs)

    @staticmethod
    def convert_tuple_string_list_to_string_list(input_list: list) -> list:
        return [str(eval(record)[0])+","+str(eval(record)[1])+","+eval(record)[2]+","+str(eval(record)[3])+"\n" for record in input_list]

    @staticmethod
    def generate_final_sorted_data(output_path:str, data_files_path:str):
        run_block_pointer = open(data_files_path+"/R_pointers.txt", 'r')
        run_initial_block = run_block_pointer.readlines()[0].strip()
        current_file = open(data_files_path+"/"+str(run_initial_block)+".txt").readlines()
        current_file_data = Utility.convert_tuple_string_list_to_string_list(current_file[:-1])
        next_file_block = current_file[-1]

        output_sorted_data = open(output_path+"/sorted_data.txt","w")
        output_sorted_data.writelines(current_file_data)

        while next_file_block != 'NULL':
            current_file = open(data_files_path+"/"+str(next_file_block)+".txt").readlines()
            current_file_data = Utility.convert_tuple_string_list_to_string_list(current_file[:-1])
            next_file_block = current_file[-1]
            output_sorted_data.writelines(current_file_data)


        output_sorted_data.close()


import os
import random
import string

#number of records to be generated
number_of_records = 65
path = os.getcwd()
dir_path = path+"/dataset"
filename = "/data.txt"
char_array = list(string.ascii_uppercase)

try:
    # create new directory
    os.makedirs(dir_path)
except OSError:
    # directory is already present
    pass

file = open(dir_path+filename,"w")


for i in range(0,number_of_records):
    record = ''
    transaction_id = i+1
    transaction_sale_amount = random.randint(1,60001)
    customer_name = ''.join([str(i) for i in random.sample(char_array,3)])
    category_of_item = random.randint(1,1501)
    record = str(transaction_id)+','+str(transaction_sale_amount)+','+customer_name+','+str(category_of_item)
    if i < number_of_records-1:
        record += '\n'
    file.write(record)

file.close()

#reading file to shuffle and create an unsorted data
# records = open(dir_path+filename).readlines()
# random.shuffle(records)
# file = open(dir_path+filename,"w")
# file.writelines(records)
# file.close()

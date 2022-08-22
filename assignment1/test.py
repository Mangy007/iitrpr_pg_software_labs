import os


path = os.getcwd()

list1 = open(path+"/dataset/data.txt").readlines()
list2 = open(path+"/sorted_run/sorted_data.txt").readlines()

list1 = [record.strip('\n').split(',')[1] for record in list1]
list2 = [record.strip('\n').split(',')[1] for record in list2]

print(set(list1)-set(list2))


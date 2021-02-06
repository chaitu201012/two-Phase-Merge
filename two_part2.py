from collections import OrderedDict
from queue import PriorityQueue 
import math
import fileinput
import sys, os
import mmap
import numpy as np
from itertools import islice
import tracemalloc
from operator import itemgetter, attrgetter
from heapq import heappush, heappop,heapify,_heapify_max as heapify_max,_heappop_max as heappop_max
import logging
import threading
import time
import resource 


###################################### Global vars ###########################

FD=""
meta_info=OrderedDict()

order_cols=OrderedDict()

partitions=0
parition_size=0
cols=""
tupe_size=0
tempfiles=[]
order="asc"


##############################################################################

def compare_func(x,y):
    keys=(values for key,values in order_cols.items())
    for i in keys:
        if x[i]==y[i]:
            continue
        if x[i]<y[i] and order=="asc":
            return True
        if x[i]>y[i] and order=="desc":
            return True
    return False



def partition(array, start, end, compare_func):
    pivot = array[start]
    low = start + 1
    high = end

    while True:
        while low <= high and compare_func(array[high], pivot):
            high = high - 1

        while low <= high and not compare_func(array[low], pivot):
            low = low + 1

        if low <= high:
            array[low], array[high] = array[high], array[low]
        else:
            break

    array[start], array[high] = array[high], array[start]

    return high


def quick_sort(array, start, end, compare_func):
    if start >= end:
        return

    p = partition(array, start, end, compare_func)
    quick_sort(array, start, p-1, compare_func)
    quick_sort(array, p+1, end, compare_func)


def limit_memory(maxsize): 
    soft, hard = resource.getrlimit(resource.RLIMIT_AS) 
    resource.setrlimit(resource.RLIMIT_AS, (maxsize, hard))


class Merger():
    """
    Algorithm based on: http://stackoverflow.com/questions/5055909/algorithm-for-n-way-merge
    """
    
    def __init__(self,output_file):
        try:
            #1. create priority queue
            self._heap = []
            self._output_file = open(output_file, 'w+')
            
        except Exception as err_msg:
            print (str(err_msg))
        
    def merge(self, input_files,order):
        try:
            # open all files
            open_files = []
            [open_files.append(open(file__, 'r')) for file__ in input_files]

            # 2. Iterate through each file f
            # enqueue the pair (nextNumberIn(f), f) using the first value as priority key
            files_dict=OrderedDict()
            temp=[(file__.readline().strip(),file__) for file__ in open_files]

            for i in temp:
                files_dict[i[1]]=prepareLine(i[0])
            for key,values in files_dict.items():
                self._heap.append(values)

            # 3. While queue not empty
            # dequeue head (m, f) of queue
            # output m
            # if f not depleted
            # enqueue (nextNumberIn(f), f)
            while(self._heap):
                keys=(values for key,values in order_cols.items())
                if order=="asc":
                    self._heap.sort(key=itemgetter(*keys))
                if order=="desc":
                    self._heap.sort(key=itemgetter(*keys),reverse=True)
        
                # get the smallest key
                smallest=[]
                smallest.append(self._heap[0])
                #print("############### line tuple "+str(smallest[0]))
                                
                # write to output file
                self._output_file.write(("  ".join(map(str,smallest[0])))+ self._delimiter_value())
                # read next line from current file
                for key,values in files_dict.items():
                    if values==smallest[0]:
                        #print(key)
                        smallest.append(key)
                        break
                    
                #print(smallest[1])
                del self._heap[0]
                read_line = smallest[1].readline()
                # check that this file has not ended
                if(len(read_line) != 0):
                    # add next element from current file
                    self._heap.append(prepareLine(read_line.strip()))
                    files_dict[smallest[1]]=prepareLine(read_line.strip())
            # clean up
            [file__.close() for file__ in open_files]    
            self._output_file.close()
                                
        except Exception as err_msg:
            print (str(err_msg))
        
    def _delimiter_value(self):
        return "\n"




class ascObject(object):
    def __init__(self, val):
        self.val = val
    def __le__(self, other):
        for key,values in order_cols.items():
            if self.val[values]==other.val[values]:
                continue
            elif self.val[values]<other.val[values]:
                return True
            else:
                return False

class descObject(object):
    def __init__(self, val):
        self.val = val
    def __ge__(self, other):
        for key,values in order_cols.items():
            if self.val[values]==other.val[values]:
                continue
            elif self.val[values]>other.val[values]:
                return True
            else:
                return False

    
def tempFiles(no):

    files=[]
    counter=0
    while(counter<no):
        filename="temp_"+str(counter)+".txt"
        filehandle=open(filename,'w')
        files.append(filename)
        filehandle.close()
        counter+=1
    return files

def prepareAscTuple(t1):
    t2= []
    for key,values in order_cols.items():
        t2.append(t1[values])
    for i in t1:
        if not i in t2:
            t2.append(i)
    return tuple(t2)

def processMetaData(metafile,cols):

    with open(metafile) as file:
        data=file.readlines()
    
    for line in data:
        temp=line.strip().split(",")
        meta_info[temp[0]]=int(temp[1])

    
    for col in cols:
        counter=0
        for key,values in meta_info.items():
            if col==key:
                order_cols[col]=counter
            counter+=1
    
    #print(meta_info)

def prepareLine(line):
    #print(line)
    offset=0
    temp=[]
    for key,values in meta_info.items():
        #print("offset--"+str(offset)+"--"+str(values))
        #print(line[offset:values+offset])
        temp.append(line[offset:values+offset])
        offset=offset+values+2
    return tuple(temp)


def mergeParitions(output_file,partitions,tempfiles,total_lines):
    print("#"*7+" phase two started "+"#"*7)
    files=[]
    files=tempfiles
    merger = Merger(output_file)
    merger.merge(files,order)
    print("#"*10+" phase two completed"+"#"*10)

def multithreadSort(file_name):
    array=[]
    keys=(values for key,values in order_cols.items())
    with open(file_name) as temp_file:
        for lineno, line in enumerate(temp_file):

            if not line:
                break

            array.append(prepareLine(line))

    temp_file.close()

    if order=="asc":
        array.sort(key=itemgetter(*keys))
    if order=="desc":
        array.sort(key=itemgetter(*keys),reverse=True)

    with open(file_name,'w') as temp_file:
        for line in array:
            write_line=("  ".join(map(str,line))+"\n")
            temp_file.write(write_line)


def created_thread_sublists(input_file,main_size,thread_number):

    
    input_file_size=os.stat(input_file).st_size
    #partitions=(math.ceil(input_file_size/main_size))
    thread_size=math.ceil(main_size/thread_number)
    partitions=math.ceil(input_file_size/thread_size)

    partition_size=math.floor(input_file_size/partitions)

    
    tuple_size=sum(int(values) for key,values in meta_info.items())
    tuple_size+=(len(meta_info)-1)*2
    no_of_tuples_per_file=math.floor(partition_size/tuple_size)
    
    total_lines=math.floor(input_file_size/tuple_size)
    if tuple_size*partitions>=main_size:
        print("No of sublists are too many and main memory is not sufficient to hold them")
        exit()
    
    #print(partitions)
    #exit()
    tempfiles=tempFiles(partitions)
    #filehandle=open(input_file,'r')
    counter=0
    read_lines=0
    endFlag=False
    lines_per_file = no_of_tuples_per_file
    smallfile = None
    file_no=0
    with open(input_file) as bigfile:
        for lineno, line in enumerate(bigfile):
            if lineno % lines_per_file == 0:
                if smallfile:
                    smallfile.close()
                small_filename = tempfiles[file_no]
                file_no+=1
                smallfile = open(small_filename, "w")

            if not line:
                endFlag=True
                break

            smallfile.write(line)

        if smallfile:
            smallfile.close()


    bigfile.close()
    
    
    file_count=0
    while True:
        if file_count==partitions:
            break
        thread_no=-1
        threads=[0]*thread_number
        
        count=0
        while count<thread_number:
            
            thread_no+=1
            
            print("**"*(7)+" sorting sublist "+str(file_count+1)+"**"*(7))
            threads[thread_no]= threading.Thread(target=multithreadSort, args=(tempfiles[file_count],))
            threads[thread_no].start()
            count+=1
            file_count+=1

        for j in threads:
            j.join()
        threads.clear()

    return partitions,tempfiles,total_lines

def createSortedSubLists(input_file,main_size):
    #print(order_cols)
    keys=(values for key,values in order_cols.items())
    #print(*keys)
    
    input_file_size=os.stat(input_file).st_size
    partitions=(math.ceil(input_file_size/main_size))
    if partitions!=1:
        partitions=4*partitions


    partition_size=math.floor(input_file_size/partitions)

    #print(str(partitions)+" number of partitions")
    #print("partitions size "+ str(partition_size))

    #identify number of lines per partition 
    tuple_size=sum(int(values) for key,values in meta_info.items())
    tuple_size+=(len(meta_info)-1)*2
    no_of_tuples_per_file=math.floor(partition_size/tuple_size)
    if tuple_size*partitions>=main_size:
        print("No of sublists are too many and main memory is not sufficient to hold them")
        exit()
    #print(no_of_tuples_per_file)
    #print(tuple_size)

    total_lines=math.floor(input_file_size/tuple_size)

    #print(total_lines)

    tempfiles=tempFiles(partitions)
    filehandle=open(input_file,'r')
    counter=0
    read_lines=0
    endFlag=False
    while True:
        keys=(values for key,values in order_cols.items())
        tuples=0
        
        filename=tempfiles[counter]
        
        array=[]
        #print(read_lines)
        dim1=min(no_of_tuples_per_file,(total_lines-read_lines))
        dim2=len(meta_info)
        #array=np.empty([dim1,dim2],dtype=str) # go for list better
        while dim1>tuples:

            line =filehandle.readline()
            
            if not line:
                endFlag=True
                break
            read_lines+=1
            line=line.strip()
            #line=line.split("  ")
            array.append(prepareLine(line))
        
            tuples+=1

        print("**"*(7)+" sorting sublist "+str(counter+1)+"**"*(7))
        if order=="asc":
            #quick_sort(array, 0, len(array)-1, compare_func)
            array.sort(key=itemgetter(*keys))
        if order=="desc":
            array.sort(key=itemgetter(*keys),reverse=True)


        file_obj=open(filename,'w')
        for i in array:
            write_line=("  ".join(map(str,i))+"\n")
            file_obj.write(write_line)   
        
        file_obj.close()

        counter+=1
        if(counter>=partitions):
            break


        ###repeat the procedure

    filehandle.close() 
    return partitions,tempfiles,total_lines



def main():
    
    if len(sys.argv)<4:
        print("Please provide all the values in the command line")
        exit()
    
    input_file=sys.argv[1]
    output_file=sys.argv[2]
    main_size=int(sys.argv[3])*math.pow(10,6)

    part1=False
    part2=False

    #print(sys.argv[4])
    #exit()
    if sys.argv[4].isdigit():
        part2=True
    else:
        part1=True
    
    thread_number=0

    if part2:
        thread_number=int(sys.argv[4])
        
        order=sys.argv[5].lower()
        cols=(" ".join(map(str,sys.argv[6:])))


 
    if part1:
        order=sys.argv[4].lower()
        cols=(" ".join(map(str,sys.argv[5:])))

    cols=cols.split()
    #print(cols)

    
    metafile=FD+"Metadata.txt"

    processMetaData(metafile,cols)  # getting columns and the size of the columns 

    if len(order_cols)==0:
        print("cols in the Metadata.txt and the provided argument cols are not matching")
        exit()

    tracemalloc.start()
    print("**"*4+" Phase one started  "+"**"*4)
    start=time.time()

    if part2:
        partitions,tempfiles,total_lines=created_thread_sublists(input_file,main_size,thread_number)

    if part1:
        partitions,tempfiles,total_lines=createSortedSubLists(input_file,main_size)


    current, peak = tracemalloc.get_traced_memory()
    print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")

    mergeParitions(output_file,partitions,tempfiles,total_lines)
    end=time.time()
    print("Time taken for total operation is "+str(end-start))

    current, peak = tracemalloc.get_traced_memory()
    print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
    tracemalloc.stop()





if __name__=='__main__':
    main()

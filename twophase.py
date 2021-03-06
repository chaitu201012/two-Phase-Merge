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
    toggle_dict=OrderedDict() # make pair <data tuple,temp file>
    # if data tuple taken out from min heap or max heap add line from that temp file and then maxheapify or min heapify
    # everytime the pop happend increase the counter values to keep track whther the total lines are written to o/p file
    #
    filehandle=open(output_file,'w')
    h=[]
    #print(partitions)

    for i in range(partitions):
        #print(str(i)+"{{{{{{{{{{{{{")
        filename=tempfiles[i]
        #print(filename)
        file_obj=open(filename,'r')
        line =file_obj.readline()
        #print(line+"}}}}}}}}}}}}}}}}}}}}")
        if not line:
            continue
        new_tuple=prepareAscTuple(prepareLine(line))
        h.append(new_tuple)
        toggle_dict[new_tuple]=filename
        #print(new_tuple)
        #exit()

        '''
        positions pushed ina list and take a counter 
        coun=0
        new_list=[]*len(meta_info)
        completed_indices=[]
        for key,values in ordered_cols.items():
            new_list[values]=new_tuple[coun]
            coun+=1
        for i in range(len(meta_info)):
            if i in completed_indices:
                continue
            new_list[i]=new_tuple[coun]
        '''
        
    if order.lower()=="asc":
        heapify(h)
    if order.lower()=="desc":
        heapify_max(h)
    
    counter=0
    while counter <total_lines or not h :
        if order.lower()=="asc":
            k=heappop(h)
            counter+=1
        if order.lower()=="desc":
            k=heappop_max(h)
            counter+=1

        next_file=toggle_dict[k]

        coun=0
        new_list=[]*len(meta_info)
        completed_indices=[]
        for key,values in order_cols.items():
            new_list[values]=new_tuple[coun]
            completed_indices.append(values)
            coun+=1
        for i in range(len(meta_info)):
            if i in completed_indices:
                continue
            new_list[i]=new_tuple[coun]
            coun+=1

        for line_number, line in enumerate(fileinput.input(next_file, inplace=1)):
            if line_number == 0:
            # do something with the line
                heappush(h,prepareAscTuple(prepareLine(line)))
            else:
                sys.stdout.write(line) # Write the remaining lines back to your file
                    
        
    write_line=("  ".join(map(str,new_list))+"\n")
    filehandle.write(write_line)
    print(new_list)
    exit()
    
    
        




def createSortedSubLists(input_file,main_size):

    keys=(values for key,values in order_cols.items())
    #print(*keys)
    
    input_file_size=os.stat(input_file).st_size
    partitions=4*(math.ceil(input_file_size/main_size))

    partition_size=math.floor(input_file_size/partitions)

    print(str(partitions)+" number of partitions")
    print("partitions size "+ str(partition_size))

    #identify number of lines per partition 
    tuple_size=sum(int(values) for key,values in meta_info.items())
    tuple_size+=(len(meta_info)-1)*2
    no_of_tuples_per_file=math.floor(partition_size/tuple_size)
    print(no_of_tuples_per_file)
    print(tuple_size)

    total_lines=math.floor(input_file_size/tuple_size)

    print(total_lines)

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

        print("#"*(counter+1)+" sorting sublist "+str(counter+1)+"#"*(counter+1))
        if order=="asc":
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

    partitions,tempfiles,total_lines=createSortedSubLists(input_file,main_size)

    current, peak = tracemalloc.get_traced_memory()
    print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")

    mergeParitions(output_file,partitions,tempfiles,total_lines)


    current, peak = tracemalloc.get_traced_memory()
    print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
    tracemalloc.stop()





if __name__=='__main__':
    main()

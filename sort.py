import sys
import os
import heapq as hq
import threading
from threading import Thread
import time

#--------------------------------------------------------------------------------------------------------#

sort_order = True     # true means ascending order
col_idx = []
meta = []             #-------> for storing metadata
total_tuples = 0

#--------------------------------------------------------------------------------------------------------#

def read_meta_data():
    metadata = open("metadata.txt","r")
    for line in metadata:
        col, size = line.strip("\n").split(",")
        temp = []
        temp.append(col)
        temp.append(size)
        meta.append(temp)
    #print(meta)

#--------------------------------------------------------------------------------------------------------#

def tuple_size():
    sum = 0
    for c in meta:
        sum+=int(c[1])
    return sum

#--------------------------------------------------------------------------------------------------------#

def get_col_idx(cols):
    idx = []
    for c in cols:
        for i in range (0,len(meta)):
            if c==meta[i][0]:
                idx.append(i)
                break
    return idx

#--------------------------------------------------------------------------------------------------------#

def sort_chunk(chunk, order, num):
    global col_idx
    new_chunk = []
    for i in range (0, len(chunk)):
        key = ""
        for idx in col_idx:
            key+=chunk[i][idx]
        #print("KEY" + key)
        new_row = []
        new_row.append(key)
        new_row.extend(chunk[i])
        new_chunk.append(new_row)
    
    if order=="asc":
        new_chunk.sort(key=lambda x:x[0])
    else:
        new_chunk.sort(key=lambda x:x[0], reverse=True)

    for i in range (0,len(new_chunk)):
        new_chunk[i] = new_chunk[i][1:]
    
    print("--> Chunk " + str(num+1) + " sorted")

    chunkName = "chunk_" + str(num+1) + ".txt"
    file = open(chunkName,'w') 
    for row in new_chunk:
        data = ""
        for token in row:
            data+=token+"  "
        data = data[:-2]
        file.write(data)
        file.write("\n")
    file.close()

#--------------------------------------------------------------------------------------------------------#

class sort_chunk_thread(Thread):

    def run(self):
        thlimit.acquire()
        try:
            global col_idx
            new_chunk = []
            for i in range (0, len(self.chunk)):
                key = ""
                for idx in col_idx:
                    key+=self.chunk[i][idx]
                #print("KEY" + key)
                new_row = []
                new_row.append(key)
                new_row.extend(self.chunk[i])
                new_chunk.append(new_row)
            
            if self.order=="asc":
                new_chunk.sort(key=lambda x:x[0])
            else:
                new_chunk.sort(key=lambda x:x[0], reverse=True)

            for i in range (0,len(new_chunk)):
                new_chunk[i] = new_chunk[i][1:]
            
            print("--> Chunk " + str(self.num+1) + " sorted")

            chunkName = "chunk_" + str(self.num+1) + ".txt"
            file = open(chunkName,'w') 
            
            for row in new_chunk:
                data = ""
                for token in row:
                    data+=token+"  "
                data = data[:-2]
                file.write(data)
                file.write("\n")
            file.close()
        finally:
            thlimit.release()
            
    def __init__(self, chunk, order, num):

        super(sort_chunk_thread, self).__init__()
        self.num = num
        self.chunk=chunk
        self.order = order
        self.start()

#--------------------------------------------------------------------------------------------------------#

def create_sorted_chunks(Num_of_rows_in_one_chunk, order, ipfilename):
    global total_tuples
    chunk = []
    num = 0
    ipfile = open(ipfilename, 'r+')
    while True:
        line = ipfile.readline()
        if not line:
            break
        row = line.strip("\n").split("  ")

        if(len(row)!=len(meta)):
            temp = []
            sr = ""
            for i in range(len(row)-2):
                sr+=row[i] + "  "
            sr = sr[:-2]
            temp.append(sr)
            for i in range(len(row)-2, len(row)):
                temp.append(row[i])
            row = temp

        total_tuples+=1
        chunk.append(row)
        if(len(chunk)==Num_of_rows_in_one_chunk):
            sort_chunk(chunk, order, num)
            num+=1
            chunk = []
            
    if(len(chunk)>0):
        sort_chunk(chunk, order, num)
        num+=1
        chunk = []
    
    return num

#--------------------------------------------------------------------------------------------------------#

def create_sorted_chunks_thread(Num_of_rows_in_one_chunk, order, ipfilename, threadCount):
    global total_tuples
    global thlimit
    thlimit = threading.BoundedSemaphore(threadCount)

    chunk = []
    num = 0
    ipfile = open(ipfilename, 'r+')
    threads = []

    while True:
        line = ipfile.readline()
        if not line:
            break
        row = line.strip("\n").split("  ")

        if(len(row)!=len(meta)):
            temp = []
            sr = ""
            for i in range(len(row)-2):
                sr+=row[i] + "  "
            sr = sr[:-2]
            temp.append(sr)
            for i in range(len(row)-2, len(row)):
                temp.append(row[i])
            row = temp

        total_tuples+=1
        chunk.append(row)
        if(len(chunk)==Num_of_rows_in_one_chunk):
            t = sort_chunk_thread(chunk, order, num)
            threads.append(t)
            num+=1
            chunk = []
            
    if(len(chunk)>0):
        t = sort_chunk_thread(chunk, order, num)
        threads.append(t)
        num+=1
        chunk = []
    
    for th in threads:
        th.join()

    return num

#--------------------------------------------------------------------------------------------------------#

def merge_sorted_chunks(Num_of_chunks, opfilename):
    opfile = open(opfilename, 'w+')
    chunks_list = []
    datalist = []
    heap = []
    tuple_count = 0
    print("\n\n--> Final merge Initialising")
    for i in range(Num_of_chunks):
        chunkptr = open("chunk_"+str(i+1)+".txt", 'r+')
        chunks_list.append(chunkptr)

    for i in range(Num_of_chunks):
        lineptr = chunks_list[i].readline()
        row = lineptr.strip("\n").split("  ")
        
        if(len(row)!=len(meta)):
            temp = []
            sr = ""
            for i in range(len(row)-2):
                sr+=row[i] + "  "
            sr = sr[:-2]
            temp.append(sr)
            for i in range(len(row)-2, len(row)):
                temp.append(row[i])
            row = temp
    
        datalist.append(row)
        key = ""
        for ci in col_idx:
            key+=row[ci]
        heap.append((key,i))        

    if sort_order:
        hq.heapify(heap)
    else:
        hq._heapify_max(heap)

    while len(heap)>0:
        tupl = ()
        if sort_order:
            tupl = hq.heappop(heap)
        else:
            tupl = hq._heappop_max(heap)
        
        index = tupl[1]
        data = datalist[index]
        if data[0]!='':
            data1 = ""
            for tkn in data:
                data1+= tkn + "  "
            data1 = data1[:-2]
            
            if tuple_count<total_tuples-1:
                data1 += "\n"
            tuple_count+=1
            opfile.write(data1)

            lineptr = chunks_list[index].readline()
            row = lineptr.strip("\n").split("  ")

            
            if(len(row)!=len(meta)):
                temp = []
                sr = ""
                for i in range(len(row)-2):
                    sr+=row[i] + "  "
                sr = sr[:-2]
                temp.append(sr)
                for i in range(len(row)-2, len(row)):
                    temp.append(row[i])
                row = temp

            if row[0]=='':
                continue

            datalist[index] = row
            key = ""
            for ci in col_idx:
                key+=row[ci]
            hq.heappush(heap, (key, index))

            if sort_order == False:
                hq._heapify_max(heap)

    opfile.close()

#--------------------------------------------------------------------------------------------------------#

def delete_temp_chunks(Num_of_chunks):
    for i in range(Num_of_chunks):
        name = "chunk_"+str(i+1)+".txt"
        os.remove(name)

#--------------------------------------------------------------------------------------------------------#

def main():
    st = time.time()

    read_meta_data()

    n = len(sys.argv)
    input_filePath  = sys.argv[1]
    output_filePath = sys.argv[2]
    main_memory_size = int(sys.argv[3])
    tkn4 = sys.argv[4]
    order = ""
    threadCount = 0
    global sort_order
    global col_idx

    print("\n-----------------------> STATS <-------------------------")
    print("\n\n MAIN MEMORY LIMIT : " + str(main_memory_size) + " MB")
    print("\n\n--> Sorting Initiated..")
    print("\n\n--> ## Running Phase 1 ##\n\n")        

    if tkn4.lower() == "asc" or tkn4.lower() == "desc":
        order = tkn4
        if order == "desc":
            sort_order = False
        cols = []
        for i in range(5,n):
            cols.append(sys.argv[i])
        col_idx = get_col_idx(cols)
        Num_of_rows_in_one_chunk = main_memory_size*1000*1000//tuple_size()
        Num_of_chunks = create_sorted_chunks(Num_of_rows_in_one_chunk, order, input_filePath)
        
    else:
        threadCount = int(tkn4)
        order = sys.argv[5]
        if order == "desc":
            sort_order = False
        cols = []
        for i in range(6,n):
            cols.append(sys.argv[i])
        col_idx = get_col_idx(cols)
        Num_of_rows_in_one_chunk = main_memory_size*1000*1000//(tuple_size()*threadCount)
        Num_of_chunks = create_sorted_chunks_thread(Num_of_rows_in_one_chunk, order, input_filePath, threadCount)
        
    print("\n\n--> ## Running Phase 2 ##")        
    merge_sorted_chunks(Num_of_chunks, output_filePath)
    delete_temp_chunks(Num_of_chunks)
    print("\n\n==> Sorting Completed.\n\n")

    print("Total execution Time : ", end = " ")
    print(time.time()-st)
    print()
    print()
#--------------------------------------------------------------------------------------------------------#

main()
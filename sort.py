import sys
import os
import heapq as hq

#--------------------------------------------------------------------------------------------------------#

sort_order = True     # true means ascending order
col_idx = []
meta = []             #-------> for storing metadata
total_tuples = 0

#--------------------------------------------------------------------------------------------------------#

def read_meta_data():
    metadata = open("metadata.txt","r")
    for line in metadata:
        col, size = line.strip().split(",")
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
    
    print("\n--> Chunk " + str(num+1) + " sorted")
    return new_chunk

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
        row = line.strip().split("  ")
        total_tuples+=1
        chunk.append(row)
        if(len(chunk)==Num_of_rows_in_one_chunk):
            chunk = sort_chunk(chunk, order, num)
            chunkName = "chunk_" + str(num+1) + ".txt"
            num+=1
            data = ""
            for row in chunk:
                for token in row:
                    data+=token+"  "
                data = data[:-2]
                data+="\n"
            file = open(chunkName,'w') 
            file.write(data[:-1])
            chunk = []
            
    if(len(chunk)>0):
        chunk = sort_chunk(chunk, order, num)
        chunkName = "chunk_" + str(num+1) + ".txt"
        num+=1
        data = ""
        for row in chunk:
            for token in row:
                data+=token+"  "
            data = data[:-2]
            data+="\n"
        file = open(chunkName,'w') 
        file.write(data[:-1])
        chunk = []
    
    return num

#--------------------------------------------------------------------------------------------------------#

def merge_sorted_chunks(Num_of_chunks, opfilename):
    opfile = open(opfilename, 'w+')
    chunks_list = []
    datalist = []
    heap = []
    tuple_count = 0
    print("\n--> Final merge Initialising")
    for i in range(Num_of_chunks):
        chunkptr = open("chunk_"+str(i+1)+".txt", 'r+')
        chunks_list.append(chunkptr)

    for i in range(Num_of_chunks):
        lineptr = chunks_list[i].readline()
        row = lineptr.strip().split("  ")
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
            row = lineptr.strip().split("  ")
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
    read_meta_data()

    n = len(sys.argv)
    input_filePath  = sys.argv[1]
    output_filePath = sys.argv[2]
    main_memory_size = int(sys.argv[3])
    order = sys.argv[4]

    global sort_order
    if order == "desc":
        sort_order = False

    cols = []
    for i in range(5,n):
        cols.append(sys.argv[i])

    print("\n-----------------------> STATS <-------------------------")
    print("\n\n MAIN MEMORY LIMIT : " + str(main_memory_size) + " MB")       
    global col_idx
    col_idx = get_col_idx(cols)

    Num_of_rows_in_one_chunk = main_memory_size*1000*1000//tuple_size()
    print("\n\n--> Sorting Initiated..")
    Num_of_chunks = create_sorted_chunks(Num_of_rows_in_one_chunk, order, input_filePath)
    merge_sorted_chunks(Num_of_chunks, output_filePath)
    delete_temp_chunks(Num_of_chunks)
    print("\n==> Sorting Completed.\n\n")

#--------------------------------------------------------------------------------------------------------#

main()
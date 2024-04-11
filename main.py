import os
import sys
from concurrent import futures
from threading import Thread

import grpc

import Kmeans_pb2_grpc
from Mapper import Mapper
from Reducer import Reducer

mapper, reducer, centroid, iteration = sys.argv[1:]
print(mapper, reducer, centroid, iteration)

mapper_ip = ["1","2","3","4","5"]
reducer_ip= ["1","2","3","4","5"]

Mappers = []
Reducers= []
for i in range(mapper):
    Mappers.append(Mapper(name="Mapper"+str(i+1),ip=mapper_ip[i]))
for i in range(reducer):
    Reducers.append(Reducer(name="Reducer"+str(i+1),ip=reducer_ip[i]))

def work_splitter():
    files = (os.listdir('./Data/Input'))
    if len(files) == 1:
        data = open(files[0], 'r')
        data = data.read().split("\n")
        size = len(data)
        alloc = size / mapper
        #Running Mapper
        map_threads=[]
        for i in range(mapper):
            th = Thread(target=Mappers[i].work_part_file,args=(i*alloc,(i+1)*alloc))
            map_threads.append(th)
        for i in map_threads:
            i.start()
        for i in map_threads:
            i.join()
        #Running Reducer
        reducer_threads=[]
        for i in range(reducer):
            th = Thread(target=Reducers[i].shufflesort)
            reducer_threads.append(th)
        for i in reducer_threads:
            i.start()
        for i in reducer_threads:
            i.join()

    else:
        print("give multiple files to mapper")


class KmeansServicer():
    pass

port="50051"
def run():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    Kmeans_pb2_grpc.add_KmeansServicer_to_server(KmeansServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
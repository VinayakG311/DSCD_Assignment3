import os
import sys
from concurrent import futures
import threading
from threading import Thread
import random

import grpc
import time

import kmeans_pb2 as Kmeans_pb2
import kmeans_pb2_grpc as Kmeans_pb2_grpc

from Servicer import KmeansServicer
from MapperInfo import Mapper
from ReducerInfo import Reducer

# mapper, reducer, centroid, iteration = sys.argv[1:]
# print(mapper, reducer, centroid, iteration)


mapper_port = ["127.0.0.1:50052","127.0.0.1:50053","127.0.0.1:50054","127.0.0.1:50055","127.0.0.1:50056","127.0.0.1:50057","127.0.0.1:50058","127.0.0.1:50059","127.0.0.1:50060","127.0.0.1:50061","127.0.0.1:50062","127.0.0.1:50063","127.0.0.1:50064","127.0.0.1:50065"]

# for i in range(1,1000):
#     port = 50051+i
#     mapper_port.append(port)
# reducer_ip= ["1","2","3","4","5"]

M,R,K,iter = [int(x) for x in sys.argv[1:5]]

Mappers = []
Reducers= []
IndicesAssigned = []

Centroid = []
for i in range(M):
    Mappers.append(Mapper(name="Mapper"+str(i+1),ip=mapper_port[i]))
    # print(Mappers[i].ip)
for i in range(R):
    Reducers.append(Reducer(name="Reducer"+str(i+1),ip=mapper_port[i+M]))

with open ("Data/Input/points.txt","r") as f:
    points = f.readlines()
    pointsList = []
    for point in points:
        pointsList.append(point.split(","))
    
    Centroid = random.sample(pointsList,K)
    
    with open("Centroid.txt","w") as g:
        for point in Centroid:
            g.write(point[0]+","+point[1])
    
    for ind in range(0,len(Centroid)):
        Centroid[ind] = Kmeans_pb2.Centroids(x_cord=float(Centroid[ind][0]),y_cord=float(Centroid[ind][1]))

def initiateMappers():
    # centroid1 = Kmeans_pb2.Centroids(data=2.5)
    # centroid2 = Kmeans_pb2.Centroids(data=3.5)
    # centroids = [centroid1,centroid2]
    
    work_splitter()
    
    for ind in range(0,len(Mappers)):
        try:
            with grpc.insecure_channel(Mappers[ind].ip) as channel:
                print(f"SENDING REQ TO {Mappers[ind].ip}")
                request = Kmeans_pb2.MasterToMapperReq(start_index=IndicesAssigned[ind][0],end_index=IndicesAssigned[ind][0],prev_Centroids=Centroid)
                stub = Kmeans_pb2_grpc.KmeansStub(channel)
                res = stub.MasterToMapper(request)
                print(f"{res}")
        except:
            continue
                



def work_splitter():
    files = (os.listdir('./Data/Input'))
    if len(files) == 1:
        data = open("Data/Input/points.txt",'r')
        data = data.read().split("\n")
        size = len(data)
        alloc = size // M
        #Running Mapper
        map_threads=[]
        for i in range(M):
            IndicesAssigned.append([i*alloc,(i+1)*alloc])
            # th = Thread(target=Mappers[i].work_part_file,args=(i*alloc,(i+1)*alloc))
            # map_threads.append(th)
        for i in map_threads:
            i.start()
        for i in map_threads:
            i.join()
        #Running Reducer
        reducer_threads=[]
        # for i in range(R):
            # th = Thread(target=Reducers[i].shufflesort)
            # reducer_threads.append(th)
        for i in reducer_threads:
            i.start()
        for i in reducer_threads:
            i.join()

    else:
        print("give multiple files to mapper")



t = []
port="50051"
def run():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    Kmeans_pb2_grpc.add_KmeansServicer_to_server(KmeansServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    
    try:
        while True:
                time.sleep(3600)  # One hour
    except KeyboardInterrupt:
        
        server.stop(0)
        sys.exit(0)

if __name__ == '__main__':
    t1 = threading.Thread(target=run)
    t2 = threading.Thread(target=initiateMappers)
    
    t.append(t1)
    t.append(t2)
    
    try:
        for i in t:
            i.start()
        for i in t:
            i.join()

    except KeyboardInterrupt:
        sys.exit(0)
        
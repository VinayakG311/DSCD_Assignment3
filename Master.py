import os
import sys
from concurrent import futures
import threading
from threading import Thread
import random

import grpc
import time

import Kmeans_pb2 as Kmeans_pb2
import Kmeans_pb2_grpc as Kmeans_pb2_grpc

from Servicer import KmeansServicer
from MapperInfo import Mapper
from ReducerInfo import Reducer
import concurrent
from concurrent import futures

# mapper, reducer, centroid, iteration = sys.argv[1:]
# print(mapper, reducer, centroid, iteration)
global success_count

mapper_port = ["127.0.0.1:50052", "127.0.0.1:50053", "127.0.0.1:50054", "127.0.0.1:50055", "127.0.0.1:50056",
               "127.0.0.1:50057", "127.0.0.1:50058"]
reducer_port = ["127.0.0.1:50059", "127.0.0.1:50060", "127.0.0.1:50061", "127.0.0.1:50062", "127.0.0.1:50063",
                "127.0.0.1:50064", "127.0.0.1:50065"]

dumpWrite = open("Dump.txt", "w")

# for i in range(1,1000):
#     port = 50051+i
#     mapper_port.append(port)
# reducer_ip= ["1","2","3","4","5"]

M, R, K, iterations = [int(x) for x in sys.argv[1:5]]

Mappers = []
Reducers = []
IndicesAssigned = []

M_count = 0

for i in range(M):
    Mappers.append(Mapper(name="Mapper" + str(i + 1), ip=mapper_port[i]))
    # print(Mappers[i].ip)
for i in range(R):
    Reducers.append(Reducer(name="Reducer" + str(i + 1), ip=mapper_port[i + M]))

Centroid = []
dumpWrite.write("Randomly Initialized Centroids - \n")
with open("Data/Input/points.txt", "r") as f:
    points = f.readlines()
    pointsList = []
    for point in points:
        pointsList.append(point.split(","))

    Centroid = random.sample(pointsList, K)
    # print(Centroid)

    with open("Centroid.txt", "w") as g:
        for point in Centroid:
            g.write(point[0] + " " + point[1])
            dumpWrite.write(point[0] + " " + point[1]+"\n")


    # for ind in range(0,len(Centroid)):
    #     Centroid[ind] = Kmeans_pb2.Centroids(x_cord=float(Centroid[ind][0]),y_cord=float(Centroid[ind][1]))


def mapperRequest(ind, success_count):
    try:
        with grpc.insecure_channel(Mappers[ind].ip) as channel:
            dumpWrite.write(f"gRPC Request sent by Master to Mapper {ind + 1} \n")
            print(f"SENDING REQ TO {Mappers[ind].ip}")
            request = Kmeans_pb2.MasterToMapperReq(mapper_index=ind + 1, start_index=IndicesAssigned[ind][0],
                                                   end_index=IndicesAssigned[ind][1], prev_Centroids=Centroid,
                                                   reducer_count=R)
            stub = Kmeans_pb2_grpc.KmeansStub(channel)
            res = stub.MasterToMapper(request)

            if (res.success == 1):

                dumpWrite.write(f"gRPC Success Response Received by Master from Mapper {ind + 1} \n")
                return True
            else:
                dumpWrite.write(f"gRPC Failure Response Received by Master from Mapper {ind + 1} \n")
                return False
            # if(res.success == 1):
            #     success_count+=1

            # print("RESPONSE RECEIVED")
            # print(f"{res}")
    except Exception:
        return


def initiateMappers(iterationNum):
    global M_count
    dumpWrite.write(f"------------{iterationNum}------------- \n")
    with open("Centroid.txt", "r") as g:
        centroids = g.readlines()
        for ind in range(0, len(centroids)):
            curr = centroids[ind].split(" ")
            Centroid[ind] = Kmeans_pb2.Centroids(x_cord=float(curr[0]), y_cord=float(curr[1]))

    success_count = 0
    work_splitter()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks for each mapper address
        futures = [executor.submit(mapperRequest, ind, success_count) for ind in range(0, M)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    count = 0
    for res in results:
        if (res):
            count += 1
    print(count)
    M_count = count

    # for ind in range(0,len(Mappers)):


def reducerRequest(i):
    try:
        with grpc.insecure_channel(reducer_port[i]) as channel:
            print(f"SENDING REQ TO REDUCER {i+1}")
            dumpWrite.write(f"gRPC Request sent by Master to Reducer {i + 1} \n")
            stub = Kmeans_pb2_grpc.KmeansStub(channel)
            req = Kmeans_pb2.MasterToReducerReq(start_process=1, id=i + 1, M=M)
            res = stub.MasterToReducer(req)
            print(res)
            if (res.success == 1):
                dumpWrite.write(f"gRPC Success Response Received by Master from Reducer {i + 1} \n")
                # reader = open(f"Data/Reducers/R{i + 1}.txt", "r")
                # writer = open("Centroid.txt", "w")
                #
                # centroids = reader.readlines()
                # centroids.sort()
                # for centroid in centroids:
                #     curr = centroid.split(" ")
                #     writer.write(curr[1] + " " + curr[2]+"\n")
                # writer.close()
            else:
                dumpWrite.write(f"gRPC Failure Response Received by Master from Reducer {i + 1} \n")
    except Exception as e:
        print(e)
        return


def initiateReducers():
    # print("REDUCER REQUEST")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks for each mapper address
        futures = [executor.submit(reducerRequest, i) for i in range(0, R)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    count = 0
    for res in results:
        if (res):
            count += 1

    dumpWrite.write("New List Of Coordinates - \n")
    data=[]
    for i in range(0, R):
        reader = open(f"Data/Reducers/R{i + 1}.txt", "r")
        centroids = reader.readlines()
        for centroid in centroids:
            data.append(centroid)
            # curr = centroid.split(" ")
            #
            # dumpWrite.write(curr[1] + " " + curr[2])
    writer=open("Centroid.txt","w")
    for i in data:
        curr = i.split(" ")
        writer.write(curr[1]+" "+curr[2]+"\n")
        dumpWrite.write(curr[1] + " " + curr[2]+"\n")
    writer.close()



def work_splitter():
    files = (os.listdir('./Data/Input'))
    if len(files) == 1:
        data = open("Data/Input/points.txt", 'r')
        data = data.read().split("\n")
        size = len(data)
        alloc = size // M
        for i in range(M):
            IndicesAssigned.append([i * alloc, (i + 1) * alloc])

    else:
        print("gave multiple files to mapper")


t = []
port = "50051"


def startKMeans():
    n = iterations
    i = 1
    while (i <= n):
        initiateMappers(i)
        if M_count == M:
            initiateReducers()
        i += 1


def condition():
    return True


def run():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    Kmeans_pb2_grpc.add_KmeansServicer_to_server(KmeansServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()

    startKMeans()

    try:
        while True:
            time.sleep(3600)  # One hour
    except KeyboardInterrupt:

        server.stop(0)
        sys.exit(0)


if __name__ == '__main__':

    t1 = threading.Thread(target=run)

    t.append(t1)

    try:
        for i in t:
            i.start()
        for i in t:
            i.join()
        dumpWrite.close()
    except KeyboardInterrupt:
        dumpWrite.close()
        sys.exit(0)

import os
import sys
from concurrent import futures
import threading
from threading import Thread

import grpc
import time

import Kmeans_pb2 as Kmeans_pb2
import Kmeans_pb2_grpc as Kmeans_pb2_grpc

mapper_port = ["127.0.0.1:50052", "127.0.0.1:50053", "127.0.0.1:50054", "127.0.0.1:50055", "127.0.0.1:50056",
               "127.0.0.1:50057", "127.0.0.1:50058"]
reducer_port = ["127.0.0.1:50059", "127.0.0.1:50060", "127.0.0.1:50061", "127.0.0.1:50062", "127.0.0.1:50063",
                "127.0.0.1:50064", "127.0.0.1:50065"]

data_mapper = {}
centroids_mapper=[]

def MapperPartition(ind, data, centroids, reducer_count):
    # If has to be read from that
    # reader = open("Data/Mappers/M"+str(ind)+"/data.txt","r")
    # data = reader.read().split("\n")

    # else using the one from servicer

    for i in range(len(data)):
        point = data[i]
        key = point[0]

        parititionFileNumber = key % reducer_count
        if parititionFileNumber in data_mapper:
            data_mapper[parititionFileNumber].append([point[0], point[1], point[2]])
        else:
            data_mapper[parititionFileNumber] = []
        f = open("Data/Mappers/M" + str(ind) + f"/Partition{parititionFileNumber}.txt", "a")
        f.write(f"{point[0]} {point[1]} {point[2]} \n")
        f.close()


def shuffle_and_sort(data):
    print("shuffle and sort")


def Reduce():
    print("Reduce")


class KmeansServicer(Kmeans_pb2_grpc.KmeansServicer):
    def MasterToMapper(self, request, context):
        print(f"Request received{request}")
        file = open("Data/Input/points.txt", "r")
        data = file.read().split("\n")[request.start_index:request.end_index]
        dict = {}
        centroids = []
        centroids_mapper=request.prev_Centroids
        for i in request.prev_Centroids:
            centroid = [i.x_cord, i.y_cord]
            centroids.append(centroid)

        for i in data:
            point = i.split(",")
            if point == ['']:
                continue
            print(point)
            point = [float(point[0]), float(point[1])]
            min_ind = -1
            min_dist = 1000000
            for j in range(len(centroids)):
                if (point[0] - centroids[j][0]) ** 2 + (point[1] - centroids[j][1]) ** 2 < min_dist:
                    min_ind = j
                    min_dist = (point[0] - centroids[j][0]) ** 2 + (point[1] - centroids[j][1]) ** 2
            if min_ind in dict:
                dict[min_ind].append(point)
            else:
                dict[min_ind] = [point]
        path = "Data/Mappers/M" + str(request.mapper_index)
        if not os.path.isdir(path):
            os.mkdir(path)

        writer = open(path + "/Data.txt", "w")
        datas = []
        for k, v in dict.items():
            for j in v:
                writer.write(str(k) + " " + str(j[0]) + " " + str(j[1]) + "\n")
                datas.append([k, j[0], j[1]])
        writer.close()
        MapperPartition(request.mapper_index, datas, centroids, request.reducer_count)
        return Kmeans_pb2.MasterToMapperRes(success=1)
        # return super().MasterToMapper(request, context)

    def ReducerToMapper(self, request, context):
        id=request.id
        data=data_mapper[id]
        reducer_Data=[]
        centroid_id=data[0][0]
        for i in data:
            d=Kmeans_pb2.Data(key=i[1],value=i[2])
            reducer_Data.append(d)

        return Kmeans_pb2.ReducerToMapperRes(success=1,data=reducer_Data,centroid=centroids_mapper[centroid_id],centroid_id=centroid_id)

    def MasterToReducer(self, request, context):
        if request.start_process != 1:
            print("Error")
            return Kmeans_pb2.MasterToReducerRes(success=0)
        # Getting data from mapper
        data = []
        for i in mapper_port:
            with grpc.insecure_channel(i) as channel:
                stub = Kmeans_pb2_grpc.KmeansStub(channel)
                req = Kmeans_pb2.ReducerToMapperReq(id=request.id)
                res = stub.ReducerToMapper(req)
                d = res.data
                keyval = []
                for i in d:
                    keyval.append([i.key, i.value])
                data.append(keyval)
        shuffle_and_sort(data)

        return Kmeans_pb2.MasterToReducerRes(success=0)

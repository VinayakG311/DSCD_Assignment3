import os
import sys
from concurrent import futures
import threading
from threading import Thread

import grpc
import time

import Kmeans_pb2 as Kmeans_pb2
import kmeans_pb2_grpc as Kmeans_pb2_grpc

def MapperPartition(ind,data,centroids,reducer_count):
    #If has to be read from that
    # reader = open("Data/Mappers/M"+str(ind)+"/data.txt","r")
    # data = reader.read().split("\n")

    #else using the one from servicer
    
    for i in range(len(data)):
        point = data[i]
        key = point[0]
        
        parititionFileNumber = key % reducer_count
        f=open("Data/Mappers/M"+str(ind)+f"/Partition{parititionFileNumber}.txt","a")
        f.write(f"{point[0]} {point[1]} {point[2]} \n")
        f.close()
    # size = len(data)
    # alloc = size/reducer_count
    # for i in range(reducer_count):
    #     f=open("Data/Mappers/M"+str(ind)+f"/Partition{i+1}.txt","w")
    #     red_data = data[i*alloc:(i+1)*alloc]
    #     for j in red_data:
    #         f.write(f"{j[0]} {j[1]} {j[2]} \n")
    #     f.close()


class KmeansServicer(Kmeans_pb2_grpc.KmeansServicer):
    def MasterToMapper(self, request, context):
        print(f"Request received{request}")
        file = open("Data/Input/points.txt", "r")
        data = file.read().split("\n")[request.start_index:request.end_index]
        dict = {}
        centroids=[]
        for i in request.prev_Centroids:
            centroid= [i.x_cord,i.y_cord]
            centroids.append(centroid)

        for i in data:
            point = i.split(",")
            point = [float(k) for k in point]
            min_ind=-1
            min_dist = 1000000
            for j in range(len(centroids)):
                if (point[0] - centroids[j][0]) ** 2 + (point[1] - centroids[j][1]) ** 2 < min_dist:
                    min_ind=j
                    min_dist= (point[0] - centroids[j][0]) ** 2 + (point[1] - centroids[j][1]) ** 2
            if min_ind in dict:
                dict[min_ind].append(point)
            else:
                dict[min_ind]=[point]
        path ="Data/Mappers/M"+str(request.mapper_index)
        if not os.path.isdir(path):
            os.mkdir(path)
        writer = open(path+"/Data.txt","w")
        datas=[]
        for k,v in dict:
            for j in v:
                writer.write(str(k)+" "+j[0]+" "+j[1]+"\n")
                datas.append([k,j[0],j[1]])
        writer.close()
        MapperPartition(datas,request.mapper_index,centroids,request.reducer_count)
        return Kmeans_pb2.MasterToMapperRes(success=1)
        # return super().MasterToMapper(request, context)

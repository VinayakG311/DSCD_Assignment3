import os
import sys
from concurrent import futures
import threading
from threading import Thread

import grpc
import time

import Kmeans_pb2 as Kmeans_pb2
import Kmeans_pb2_grpc as Kmeans_pb2_grpc

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
            if point==['']:
                continue
            print(point)
            point = [float(point[0]),float(point[1])]
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
        for k,v in dict.items():
            for j in v:
                writer.write(str(k)+" "+str(j[0])+" "+str(j[1])+"\n")
                datas.append([k,j[0],j[1]])
        writer.close()
        MapperPartition(request.mapper_index,datas,centroids,request.reducer_count)
        return Kmeans_pb2.MasterToMapperRes(success=1)
        # return super().MasterToMapper(request, context)

    def MapperToReducer(self, request, context):
        print(f"Request received{request}")
        return Kmeans_pb2.MapperToReducerRes(success=1)
        # return super().MapperToReducer(request, context)

    def ReducerToMapper(self, request, context):
        print(f"Request received{request}")
        Key = request.centroid_id
        Value = request.centroid_values_list
        key_values = request.key_values

        #sort----------------------------
        l_uniques_keys = list(set(key_values.keys()))
        l_uniques_keys.sort()
        sorted_dict = {}
        for i in l_uniques_keys:
            sorted_dict[i] = []
        
        for i in l_uniques_keys:
            sorted_dict[i].append(key_values[i])
        print(sorted_dict)
        #---------------------------------

        #write to file
        updated_centroids = []
        for i in range(len(sorted_dict)):
            sum_x = 0
            sum_y = 0
            count = 0
            for j in sorted_dict[i]:
                sum_x+=j[0]
                sum_y+=j[1]
                count+=1
            updated_centroids.append([sum_x/count,sum_y/count])
        
        with open("Data/Reducers/R"+str(request.reducer_index)+"/Centroid.txt","w") as f:
            for i in updated_centroids:
                f.write(f"{i[0]} {i[1]} \n")
        

        return Kmeans_pb2.ReducerToMapperRes(success=1)
        # return super().ReducerToMapper(request, context)   

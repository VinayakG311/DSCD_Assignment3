import os
import sys
from concurrent import futures
import threading
from threading import Thread

import grpc
import time

import kmeans_pb2 as Kmeans_pb2
import kmeans_pb2_grpc as Kmeans_pb2_grpc

class KmeansServicer(Kmeans_pb2_grpc.KmeansServicer):
    def MasterToMapper(self, request, context):

            print(f"Request received{request}")
            
            return Kmeans_pb2.MasterToMapperRes(success=1)
        # return super().MasterToMapper(request, context)

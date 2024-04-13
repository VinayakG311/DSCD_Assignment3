import sys

import grpc

import kmeans_pb2_grpc as Kmeans_pb2_grpc 

ip = sys.argv[1]
class Reducer():
    name=""
    ip=""
    mapper_count= 0
    def __init__(self,name,ip):
        self.name=name
        self.ip=ip

    def shufflesort(self):
        pass


def run():
    reducer = Reducer(name="",ip=ip)
    with grpc.insecure_channel(ip) as channel:
        stub = Kmeans_pb2_grpc.KmeansStub(channel)
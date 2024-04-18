import grpc
import sys
import threading
import time
from concurrent import futures
from Servicer import KmeansServicer

import Kmeans_pb2 as Kmeans_pb2
import kmeans_pb2_grpc as Kmeans_pb2_grpc

port = sys.argv[1]

masterIP = "127.0.0.1:50051"

class Mapper:

    name=""
    reducer_count=0
    ip=""
    busy = False
    

    def __init__(self,name,ip):
        self.name=name

        self.ip = ip

    def work_part_file(self,file, index_start, index_end):
        data = open(file)
        data = data.read().split("\n")[index_start:index_end]

    def work_full_file(self,file):
        pass

    def Map(self,data,centroids_prev,other_info):
        #Do work and store in output
        output={"lorem ipsum":1}
        #If needed to write Map output to file
        # writer = open(self.name+"/mapper_data.txt","w")
        # writer.write(output)
        self.Partition(output)
    def Partition(self,data):
        #If needed to read map output from file
        reader = open(self.name+"/mapper_data.txt","r")
        reader=reader.read().split("\n")
        output={}
        for i in reader: #or data
            d=i.split(" ")
            output[d[0]].append(d[1])
        num_keys = len(output.keys())
        red_keys = num_keys/self.reducer_count
        for i in range(self.reducer_count):
            file = open(self.name+"/partition_"+str(i+1))
            #write to partiiton
            
# class KmeansServicer(Kmeans_pb2_grpc.KmeansServicer):
#     def MasterToMapper(self, request, context):

#             print(f"Request received{request}")
            
#             return Kmeans_pb2.MasterToMapperRes(success=1)
#         # return super().MasterToMapper(request, context)


t = []
def run():
    # mapper = Mapper(name="mapper",ip=f"[::]:{port}")
    with grpc.insecure_channel(masterIP) as channel:
        stub = Kmeans_pb2_grpc.KmeansStub(channel)
        # print("STUB CREATED")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    Kmeans_pb2_grpc.add_KmeansServicer_to_server(KmeansServicer(), server)
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    
    try:
        while True:
                time.sleep(3600)  # One hour
    except KeyboardInterrupt:
        
        server.stop(0)
        sys.exit(0)
    
    
if __name__ == '__main__':
    t1 = threading.Thread(target=run)
    t2 = threading.Thread(target=serve)
    
    t.append(t2)
    t.append(t1)
    
    try:
        for i in t:
            i.start()
        for i in t:
            i.join()

    except KeyboardInterrupt:
        sys.exit(0)
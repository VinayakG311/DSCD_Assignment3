import grpc
import sys
import threading
import time
from concurrent import futures
from Servicer import KmeansServicer

import Kmeans_pb2 as Kmeans_pb2
import Kmeans_pb2_grpc as Kmeans_pb2_grpc
masterIP = "127.0.0.1:50051"
port = sys.argv[1]
class Reducer():
    name=""
    ip=""
    mapper_count= 0
    def __init__(self,name,ip):
        self.name=name
        self.ip=ip

    def shufflesort(self):
        pass


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
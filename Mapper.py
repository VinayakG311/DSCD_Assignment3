


class Mapper:

    name=""
    reducer_count=0
    def __init__(self,name,reducer_count):
        self.name=name
        self.reducer_count=reducer_count
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
        pass




import os
import sys

from Mapper import Mapper

mapper, reducer, centroid, iteration = sys.argv[1:]
print(mapper, reducer, centroid, iteration)


Mappers = []
for i in range(reducer):
    Mappers.append(Mapper(name="Mapper"+str(i+1)))
def work_splitter():
    files = (os.listdir('./Data/Input'))
    if len(files) == 1:
        data = open(files[0], 'r')
        data = data.read().split("\n")
        size = len(data)
        alloc = size / mapper
        for i in range(reducer):
            Mappers[i].work_part_file(files[0], i * alloc, (i + 1) * alloc)
    else:
        print("give multiple files to mapper")



work_splitter()

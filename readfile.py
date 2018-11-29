import os
import sys
import json
import struct

from ross_vis.DataModel import RossData
from ross_vis.DataCache import RossDataCache as Cache

def readDataFromFile(filename, includes = ['PeData'], excludes = ['ModelData']):
    reader = RossData(includes, excludes)
    with open(filename, "rb") as binary_file:
        # Read the whole file at once
        buf = binary_file.read()
        data = reader.readall(buf)

    return data

if __name__ == '__main__':

    if (len(sys.argv) < 2):
        print('Usage: %s <ross_data_filename>' % sys.argv[0])
    else:
        if (os.path.isfile(sys.argv[1])):

            if (len(sys.argv) > 2 and sys.argv[2] in ['PeData', 'KpData', 'LpData']):
                attr = sys.argv[2]
            else: 
                attr = 'PeData'
            # data = readDataFromFile(sys.argv[1])
            # print(json.dumps(data, indent=4))
            cache = Cache()
            cache.loadfile(sys.argv[1])
            print(cache.export_json(attr))
           
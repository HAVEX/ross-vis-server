from json import dumps
from ross_vis.DataModel import RossData
from ross_vis.Transform import flatten_list

class RossDataCache:

    def __init__(self, data = [], max_size = 1000):
        self.data = data
        self.max_size = max_size

    def size(self):
        return len(self.data)
    
    def push(self, sample):
        if(len(self.data) > self.max_size):
            self.data.pop(0)
        self.data.append(sample)
    
    def get(self, attribute):
        dm = RossData([attribute])
        results = []
        for row in self.data:
            results.append(dm.fetch(row))
        
        return flatten(results)

    def loadfile(self, filename):
        with open(filename, "rb") as binary_file:
            # Read the whole file at once
            buf = binary_file.read()
            self.data = RossData().get_samples(buf)

        return self.data

    def export_json(self, include = None):
        dm = RossData([include])
        results = []
        for row in self.data:
            results.append(dm.fetch(row))
        
        return dumps(flatten_list(results), indent=4)
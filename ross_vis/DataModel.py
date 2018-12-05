import struct
import ross_damaris.sample.DamarisDataSample as RossSample

class RossData:
    FLATBUFFER_OFFSET_SIZE = 4

    def __init__(self, includes = [], excludes = ['ModelData']):
        self.includes = includes
        self.excludes = excludes
    
    def get_samples(self, bufArray):
        arrayLength = len(bufArray)
        SIZE_T = RossData.FLATBUFFER_OFFSET_SIZE
        offset = 0
        samples = list()
        
        while offset < arrayLength:
            bufSize = struct.unpack('i', bufArray[offset:offset+SIZE_T])[0]
            offset += SIZE_T
            data = bufArray[offset:offset+SIZE_T+bufSize]
            offset += bufSize
            samples.append(data)

        return samples

    def size(self, dataBuf):
        bufSize = struct.unpack('i', dataBuf[0:RossData.FLATBUFFER_OFFSET_SIZE])[0]
        return bufSize

    def isValid(self, dataBuf):
        if(len(dataBuf) <= self.size(dataBuf) + RossData.FLATBUFFER_OFFSET_SIZE):
            return True
        else:
            return False

    def read(self, dataBuf):
        if (self.isValid(dataBuf)):
            return self.fetch(dataBuf[RossData.FLATBUFFER_OFFSET_SIZE:])


    def fetch(self, dataBuf):
        data = RossSample.DamarisDataSample.GetRootAsDamarisDataSample(dataBuf, 0)
        return self.decode(data)

    def decode(self, data):
        result = {}
        method_names = [
            method for method in dir(data) 
            if callable(getattr(data, method)) 
            and not method.startswith(('__', 'GetRootAs', 'Init'))
        ]
        
        for name in method_names:
            if (name.endswith('DataLength') or name in self.excludes):
                continue

            method = getattr(data, name)
            if(not name.endswith('Data')):
                result[name] = method()
            elif(name == 'Data'):
                result[name] = self.decode(method())
            else:
                if(len(self.includes) == 0 or name in self.includes):
                    getLen = getattr(data, name+'Length')
                    dataLength = getLen()
                    result[name] = list()
                    for ii in range(0, dataLength):
                        result[name].append(self.decode(method(ii)))

        return result


    def readall(self, bufArray):
        arrayLength = len(bufArray)
        SIZE_T = RossData.FLATBUFFER_OFFSET_SIZE
        offset = 0
        results = list()

        while offset < arrayLength:
            if(offset+SIZE_T>arrayLength):
                break
            bufSize = struct.unpack('i', bufArray[offset:offset+SIZE_T])[0]
            
            # in case data is incomplete, drop the remaining of data in the file
            if(offset+SIZE_T+bufSize>arrayLength):
                break
            data = bufArray[offset:offset+SIZE_T+bufSize]
            offset += (SIZE_T + bufSize)
            
            result = self.read(data)
            results.append(result)

        return results

    def flatten(self, data):
        flattens = []
        for di in data:
            flat = {}
            for key,value in di.items():
                if(type(value) != list):
                    flat[key] = value
                else:
                    for nestedValues in value:
                        flatNested = flat.copy()
                        for nestedKey,nestedVal in nestedValues.items():
                            if(type(nestedVal) != dict):
                                flatNested[nestedKey] = nestedVal
                            else:
                                for a,v in nestedVal.items():
                                    flatNested[a] = v
                
                        flattens.append(flatNested)

        return flattens
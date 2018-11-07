import os, sys
import flatbuffers
import struct
import json

import ross_damaris.sample.DamarisDataSample as ross
import ross_damaris.sample.LPData as lps
import ross_damaris.sample.KPData as kps
import ross_damaris.sample.SimEngineMetrics as metrics

FLATBUFFER_OFFSET_SIZE = 4

# TODO: Create a class for all the functions in this file

def decodeRossFlatBuffer(bufObj, includes, excludes):
    result = {}

    method_names = [
        method for method in dir(bufObj) 
        if callable(getattr(bufObj, method)) 
        and not method.startswith(('__', 'GetRootAs', 'Init'))
    ]
    
    for name in method_names:

        if (name.endswith('DataLength') or name in excludes):
            continue

        method = getattr(bufObj, name)

        if(not name.endswith('Data')):
            result[name] = method()
        elif(name == 'Data'):
            result[name] = decodeRossFlatBuffer(method(), includes, excludes)
        else:
            if(name in includes):
                getLen = getattr(bufObj, name+'Length')
                dataLength = getLen()
                result[name] = list()
                for ii in range(0, dataLength):
                    result[name].append(decodeRossFlatBuffer(method(ii), includes, excludes))

    return result

def getSampleSize(buf):
    bufSize = struct.unpack('i', buf[0:FLATBUFFER_OFFSET_SIZE])[0]
    return bufSize

def isSampleValid(buf):
    if(len(buf) <= getSampleSize(buf) + FLATBUFFER_OFFSET_SIZE):
        return True
    else:
        return False

def processRawDataSample(buf, includes = ['PeData'], excludes = ['ModelData']):
    result = {}
    if (isSampleValid(buf)):
        dataBuf = ross.DamarisDataSample.GetRootAsDamarisDataSample(buf[FLATBUFFER_OFFSET_SIZE:], 0)
        result = decodeRossFlatBuffer(dataBuf, includes, excludes)

    return result

def readRossDataSample(sampleBuf):
        dataBuf = ross.DamarisDataSample.GetRootAsDamarisDataSample(sampleBuf, 0)
        result = decodeRossFlatBuffer(dataBuf, includes, excludes)

def getFlatbufferOffsetSize():
    return FLATBUFFER_OFFSET_SIZE

def readDataFromFile(filename, includes = ['PeData', 'KpData'], excludes = ['ModelData']):
    with open(filename, "rb") as binary_file:
        # Read the whole file at once
        buf = binary_file.read()
        fileSize = len(buf)
        offset = 0
        results = list()

        while offset < fileSize:
            bufSize = struct.unpack('i', buf[offset:offset+FLATBUFFER_OFFSET_SIZE])[0]
            if(offset+bufSize > fileSize):
                break
            data = buf[offset:offset+FLATBUFFER_OFFSET_SIZE+bufSize]
            offset += (FLATBUFFER_OFFSET_SIZE + bufSize)
            result = processRawDataSample(data, includes, excludes)
            results.append(result)

    return result

if __name__ == '__main__':

    if (len(sys.argv) < 2):
        print('Usage: %s <ross_data_filename>' % sys.argv[0])
    else:
        if (os.path.isfile(sys.argv[1])):
            data = readDataFromFile(sys.argv[1])
            print(json.dumps(data, indent=4))
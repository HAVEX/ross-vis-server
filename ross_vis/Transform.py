import itertools

def extract(data, attribute):
    results = []
    for di in data:
        if attribute in di:
            if(type(di[attribute]) == list):
                for d in di[attribute]:
                    results.append(d)
            else:
                results.append(di[attribute])

    return results

def flatten(obj, attribute = None):
    flatData = []
    flat = {}
    for key,value in obj.items():
        if(type(value) != list):
            flat[key] = value
        else:
            if(attribute != None and key != attribute):
                continue
            for nestedValues in value:
                flatNested = flat.copy()
                for nestedKey,nestedVal in nestedValues.items():
                    if(type(nestedVal) != dict):
                        flatNested[nestedKey] = nestedVal
                    else:
                        for a,v in nestedVal.items():
                            flatNested[a] = v
        
                flatData.append(flatNested)

    return flatData

def flatten_list(data, attribute = None):
    flatlist = []
    for di in data:
        flatlist.append(flatten(di))
        
    return list(itertools.chain.from_iterable(flatlist))
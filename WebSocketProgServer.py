import urllib
import json
import tornado.websocket
import time
import base64

from ross_vis.DataModel import RossData
from ross_vis.DataCache import RossDataCache
from ross_vis.Transform import flatten, flatten_list
from ross_vis.ProgAnalytics import StreamData

""" 
Parallelism wont work because there is no memory sharing. 
def processWorker(stream_count, algo, time_domain, granularity, stream, metric):
    if stream_count == 0:  
        cpd = CPD()
        pca = PCA()
        causal = Causal()
        clustering = Clustering()
        prop_data = {}
    else: 
        prop_data = stream_data.update(stream)
        cpd_result = cpd.tick(stream_data, algo.cpd)
        pca_result = pca.tick(stream_data, algo.pca)
        clustering_result = clustering.tick(stream_data)
        #causal.tick(stream_data, algo.causality)
        msg = {
            '_data': prop_data,
            'cpd' : cpd_result,
            'pca': pca_result,
            'clustering': clustering_result,
        }
        return msg  

pool = multiprocessing.Pool()  
stream_data = StreamData(stream, self.granularity, self.time_domain)
func = partial(process, stream_data, self.data_count, self.algo, self.time_domain, self.granularity, stream)
msg = pool.map(func, self.metric)
 """
class WebSocketHandler(tornado.websocket.WebSocketHandler):
    waiters = set()
    cache = RossDataCache()
    cache_size = 100
    KpData = []
    params = None

    def runOnServer(self):
        stream_objs = {}
        def process(stream, stream_count):
            metric = ['RbSec']
            granularity = 'KpGid'
            time_domain = 'LastGvt'
            algo = {
                'cpd': 'aff',
                'pca': 'prog_inc',
                'causality': 'var',
                'clustering': 'evostream',
            }  
            ret = {}                  
            for idx, metric in enumerate(metric):
                print('Calculating results for {0}'.format(metric))
                if stream_count == 0: 
                    stream_data = StreamData(stream, granularity, metric, time_domain)
                    stream_objs[metric] = stream_data
                    ret[metric] = stream_data.format()
                    ret['data'] = [{}, {}]
                elif stream_count < 2: 
                    stream_obj = stream_objs[metric]
                    stream_data = stream_obj.update(stream)
                    ret[metric] = stream_data.format()
                    ret['data'] = stream_obj.comm_data()
                else:
                    stream_obj = stream_objs[metric]
                    stream_data = stream_obj.update(stream)
                    ret[metric] = stream_obj.run_methods(stream_data, algo)
                    ret['data'] = stream_obj.comm_data()
            return ret 

        data_attribute = 'KpData'
        max_stream_count = 100
        for stream_count in range(0, max_stream_count):
            rd = RossData([data_attribute])
            sample = WebSocketProgHandler.cache.data[stream_count]
            stream = flatten(rd.fetch(sample))
            res = process(stream, stream_count)

    def open(self):
        self.data_attribute = 'PeData'
        self.method = 'get' 
        self.granularity = 'Peid'
        self.metric = ['NeventProcessed']
        self.time_domain = 'LastGvt'
        self.algo = {
            'cpd': 'aff',
            'pca': 'prog_inc',
            'causality': 'var',
            'clustering': 'evostream',
        }
        self.stream_count = 0
        self.stream_objs = {}
        self.max_stream_count = 20
        self.update = 1
        WebSocketHandler.waiters.add(self)

    def process(self, stream):  
        ret = {}                  
        for idx, metric in enumerate(self.metric):
            print('Calculating results for {0}'.format(metric))
            if self.stream_count == 0: 
                self.stream_data = StreamData(stream, self.granularity, metric, self.time_domain)
                self.stream_objs[metric] = self.stream_data
                ret[metric] = self.stream_data.format()
                ret['data'] = [{}, {}]
            elif self.stream_count < 2: 
                stream_obj = self.stream_objs[metric]
                self.stream_data = stream_obj.update(stream)
                ret[metric] = self.stream_data.format()
                ret['data'] = stream_obj.comm_data()
            else:
                stream_obj = self.stream_objs[metric]
                if(self.update == 1):
                    self.stream_data = stream_obj.update(stream)
                else:
                    self.stream_data = stream_obj.deupdate(stream)
                ret[metric] = stream_obj.run_methods(self.stream_data, self.algo)
                ret['data'] = stream_obj.comm_data()
        return ret 
    
    def pre_calc(self):
        for idx, metric in enumerate(self.metric):
            ret = {}
            filename = self.stream_count + self.metric + '.csv'
            print("Reading from {0}".format(filename))
            results = pd.read_csv(filename)
            schema = {k:self.process_type(type(v).__name__) for k,v in self.df.iloc[0].items()}
            ret[metric] = (results.to_dict('records'), schema)
        return ret

    def on_message(self, message, binary=False):
        req = json.loads(message)
        print(message)

        if('data' in req and req['data'] in ['PeData', 'KpData', 'LpData']):
            self.data_attribute = req['data']

        if('method' in req and req['method'] in ['stream', 'get', 'set', 'get-count', 'pre-calc']):
            self.method = req['method']
        
        if('granularity' in req and req['granularity'] in ['Peid', 'KpGid', 'Lpid', 'Kpid']):
            self.granularity = req['granularity']

        if('timeDomain' in req and req['timeDomain'] in ['LastGvt', 'VirtualTime', 'RealTs']):
            self.time_domain = req['timeDomain']

        if('cpdMethod' in req and req['cpdMethod'] in ['aff', 'stream']):
            self.algo.cpd = req['cpdMethod']

        if('pcaMethod' in req and req['pcaMethod'] in ['prog_inc', 'inc']):
            self.algo.pca = req['pcaMethod']

        if('causalityMethod' in req and req['causalityMethod'] in ['var']):
            self.algo.causality = req['causalityMethod']

        if('clusteringMethod' in req and req['clusteringMethod'] in ['evostream']):
            self.algo.clustering = req['clusteringMethod']

        if('metric' in req):
            self.metric = req['metric']   

        if('stream_count' in req):
            self.stream_count = req['stream_count']

        if('update' in req):
            self.update = req['update']

        if(self.method == 'stream'):
            rd = RossData([self.data_attribute])
            sample = WebSocketHandler.cache.data[self.stream_count]
            stream = flatten(rd.fetch(sample))
            res = self.process(stream)
            msg = {}
            for idx, metric in enumerate(self.metric):
                r = res.get(metric)
                result = r[0]
                schema = r[1]
               
                msg[metric] = {
                    'result': r[0],
                    'schema': r[1]
                }

            if(self.stream_count > 0):
                msg['comm'] = res.get('data')
            self.write_message(msg)

        if(self.method == 'pre-calc'):
            res = self.pre_calc()
            msg = {}
            for idx, metric in enumerate(self.metric):
                r = res.get(metric)
                result = r[0]
                schema = r[1]
                msg[metric] = {
                    'result': result,
                    'schema': schema
                }
            self.write_message(msg)

        if(self.method == 'stream-next'):
            rd = RossData([self.data_attribute])
            sample = WebSocketHandler.cache.data.pop(0)
            msg = {'data': flatten(rd.fetch(sample))}
            self.write_message(msg)

        if(self.method == 'get'):
            data = WebSocketHandler.cache.export_dict(self.data_attribute)
            schema = {k:type(v).__name__ for k,v in data[0].items()}
            self.write_message({
                'data': data,
                'schema': schema
            })
            
            msg = {'data': data, 'schema': schema}
            if(WebSocketHandler.params != None):
                msg['params'] = self.params
            self.write_message(msg)


        if(self.method == 'get-count'):
            data = WebSocketHandler.cache.export_dict_count(self.data_attribute, self.stream_count)
            schema = {k: type(v).__name__ for k, v in data[0].items()}
            self.write_message({
                'data': data,
                'schema': schema
            })

        if(self.method == 'set'):
            WebSocketHandler.params = req['params']
            self.write_message({'status': 'ok'})
            print(WebSocketHandler.params)

    def on_close(self):
        print('connection closed')
        WebSocketHandler.waiters.remove(self)

    def check_origin(self, origin):
        return True
        # parsed_origin = urllib.parse.urlparse(origin)
        # return parsed_origin.netloc.startswith("localhost:")

    @classmethod
    def push_updates(cls, data):
        for waiter in cls.waiters:
            try:
                waiter.write_message(data)
            except:
                logging.error("Error sending message", exc_info=True)        

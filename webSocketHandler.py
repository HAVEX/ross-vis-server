import urllib
import json
import tornado.websocket
import time

from ross_vis.DataModel import RossData
from ross_vis.DataCache import RossDataCache
from ross_vis.Transform import flatten, flatten_list
from ross_vis.ProgAnalytics import ProgAnalytics

class WebSocketHandler(tornado.websocket.WebSocketHandler):
    waiters = set()
    cache = RossDataCache()
    cache_size = 100

    def open(self):
        print('new connection')
        self.data_attribute = 'PeData'
        self.method = 'get' 
        self.granularity = 'Peid'
        self.metric = 'RbSec'
        self.time_domain = 'LastGvt'
        self.data_count = 0
        self.max_data_count = 100
        WebSocketHandler.waiters.add(self)

    def on_message(self, message, binary=False):
        # print('message received %s' % message)
        req = json.loads(message)

        if('data' in req and req['data'] in ['PeData', 'KpData', 'LpData']):
            self.data_attribute = req['data']

        if('method' in req and req['method'] in ['stream', 'get']):
            self.method = req['method']
        
        if('granularity' in req and req['granularity'] in ['Peid', 'KpGid', 'Lpid']):
            self.granularity = req['granularity']

        if('timeDomain' in req and req['timeDomain'] in ['LastGvt', 'VirtualTime', 'RealTs']):
            self.time_domain = req['timeDomain']

        if('metric' in req):
            self.metric = req['metric']   

        if(self.method == 'stream'):
            rd = RossData([self.data_attribute])
            for sample in WebSocketHandler.cache.data:
                if self.data_count < self.max_data_count:
                    data = flatten(rd.fetch(sample))
                    schema = {k:type(v).__name__ for k,v in data[0].items()}
                    if self.data_count == 0: 
                        analysis = ProgAnalytics(data, self.granularity, self.metric, self.time_domain)
                    else: 
                        analysis.update(data, self.granularity, self.metric, self.time_domain)
                    time.sleep(0.5)
                    msg = {
                        'data': data,
                        'schema': schema
                    }
                    print(self.data_count)
                    self.data_count = self.data_count + 1
                    self.write_message(msg)
                else:
                    print('writing to csv')
                    analysis.to_csv()
                    self.on_close()

        if(self.method == 'stream-test'):
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

    def on_close(self):
        print('connection closed')
        WebSocketHandler.waiters.remove(self)

    def check_origin(self, origin):
        # return True
        parsed_origin = urllib.parse.urlparse(origin)
        return parsed_origin.netloc.startswith("localhost:")

    @classmethod
    def push_updates(cls, data):
        for waiter in cls.waiters:
            try:
                waiter.write_message(data)
            except:
                logging.error("Error sending message", exc_info=True)        

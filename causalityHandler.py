import tornado.web
import tornado.escape
import json
import numpy as np
import pandas as pd
from ross_vis.Analytics import Analytics

from webSocketHandler import WebSocketHandler

class CausalityHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def get(self):
        data = WebSocketHandler.cache.export_dict('KpData')
        # analysis
        analysis = Analytics(data, index=['NetworkRecv', 'NetworkSend', 'NeventProcessed', 'NeventRb', \
            'RbSec', 'RbTotal', 'VirtualTimeDiff'])
        #analysis.groupby(['Peid', str(metric)])
        
        analysis.causality()

        schema = {k:type(v).__name__ for k,v in data[0].items()}
        self.write({
            'data': result.to_dict('records'),
            'schema': schema
        })


"""         # parsing the parameters
        metrics_in_view_byte_format = self.request.arguments.get('metrics[]')
        metric = metrics_in_view_byte_format[0].decode('utf8').replace("'", "")        
        method_in_byte_format = self.request.arguments.get('method')[0]
        method = method_in_byte_format.decode('utf8').replace("'", "")
        print("Computing PC components for {1} using {0}".format(method, metric)) """

      
import tornado.web
import tornado.escape
import json
import numpy as np
import pandas as pd
from ross_vis.Analytics import Analytics

from webSocketHandler import WebSocketHandler

class PCAHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def get(self):
        data = WebSocketHandler.cache.export_dict('KpData')

        # parsing the parameters
        metrics_in_view_byte_format = self.request.arguments.get('metrics[]')        
        method_in_byte_format = self.request.arguments.get('method')[0]
        method = method_in_byte_format.decode('utf8').replace("'", "")
        print("Computing PC components using {0}".format(method))

        # analysis
        analysis = Analytics(data, index=['Peid', 'Kpid', 'RealTs', 'LastGvt', 'VirtualTs', 'KpGid', 'EventId'])
        analysis.groupby(['Peid', 'Kpid'])
        if method == "PCA":            
            result = analysis.pca(2)
        elif method == "prog_inc_PCA":
            result = analysis.prog_inc_pca(2, 0.1)
        elif method == "inc_PCA":
            result = analysis.inc_pca()
        elif method == "tsne":
            result == analysis.a_tsne()
        schema = {k:type(v).__name__ for k,v in data[0].items()}
        self.write({
            'data': result.to_dict('records'),
            'schema': schema
        })

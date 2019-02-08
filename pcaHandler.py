import tornado.web
import tornado.escape
import json
import numpy as np
from ross_vis.Analytics import Analytics

from webSocketHandler import WebSocketHandler

class PCAHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def get(self):
        data = WebSocketHandler.cache.export_dict('KpData')
        analysis = Analytics(data, index=['Peid', 'Kpid', 'RealTs', 'LastGvt', 'VirtualTs', 'KpGid', 'EventId'])
        analysis.groupby(['Peid', 'Kpid'])
        result = analysis.pca(2)
        schema = {k:type(v).__name__ for k,v in data[0].items()}
        self.write({
            'data': result.to_dict('records'),
            'schema': schema
        })

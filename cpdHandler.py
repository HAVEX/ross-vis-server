import tornado.web
import tornado.escape
import json
import numpy as np
from ross_vis.Analytics import Analytics

from webSocketHandler import WebSocketHandler

class CPDHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def get(self):
        data = WebSocketHandler.cache.export_dict('PeData')

        # tornado's RequestHandler converts to byte[] format. Meh! 
        time_domain = self.request.arguments.get('timeDomain')[0].decode('utf8').replace("'","")
        y_domain = self.request.arguments.get('yDomain')[0].decode('utf8').replace("'","")

        print(y_domain, time_domain)

        analysis = Analytics(data, None)
        analysis.groupby([time_domain, 'Peid'])

        result = analysis.pca_stream_cpd(y_domain)


    def find_change_points_aff(self, time_series, x_attr):
        alpha = 0.05
        eta = 0.1
        bl = 50
        aff = aff_cpp.AFF(alpha, eta, bl)
        
        cp = {}        
        for idx, metric in enumerate(self.metrics_in_view):
            y_series = self.dict_to_list_by_key(time_series, metric)
            x_series = self.dict_to_list_by_key(time_series, x_attr)
            print(x_series, y_series)
            x_min, x_max = np.amin(x_series), np.amax(x_series)
            lin_space = np.linspace(x_min, x_max, len(x_series))
            xy_series = np.interp(lin_space, x_series, y_series)                                                
            
            xy_series_np = np.asarray(xy_series)
            cp[metric] =  aff.process(xy_series_np)[1]            
        return cp


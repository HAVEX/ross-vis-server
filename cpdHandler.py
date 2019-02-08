import tornado.web
import tornado.escape
import json
import numpy as np

from change_point_detection.ffstream import aff_cpp
from webSocketHandler import WebSocketHandler

class CPDHandler(tornado.web.RequestHandler):
    def get(self):
        # tornado's RequestHandler converts to byte[] format. Meh! 
        metrics_in_view_byte_format = self.request.arguments.get('metrics[]')
        # Decode the byte format into a list. 
        self.metrics_in_view = []
        for idx, metrics in enumerate(metrics_in_view_byte_format):
            self.metrics_in_view.append(metrics.decode('utf8').replace("'", '"'))
            self.metrics_in_view.append('LastGvt')
        self.pes = [0, 1, 2]
        self.process_data()
        self.x_attr = 'LastGvt'
        self.cp = self.find_change_points_aff(self.pe0, self.x_attr)
        self.write(self.cp)

    def dump_by_metrics(self, obj):
        ret = {}
        for idx, metric in enumerate(self.metrics_in_view):
            ret[metric] = obj[metric]
        return ret
        
    # hard coding for now. 
    def process_data(self):
        self.data = WebSocketHandler.cache.export_dict('KpData')
        self.pe0 = self.pe1 = self.pe2 = []
        for idx, lp in enumerate(self.data):
            metric_data = self.dump_by_metrics(lp)
            if lp['Peid'] == 0:
                self.pe0.append(metric_data)
            elif lp['Peid'] == 1:
                self.pe1.append(metric_data)
            elif lp['Peid'] == 2:
                self.pe2.append(metric_data)
        


    def dict_to_list_by_key(self, data, metric):        
        ret = []
        for idx, lp in enumerate(data):
            ret.append(float(lp[metric]))
        return ret
    
    def find_change_points_aff(self, time_series, x_attr):
        alpha = 0.05
        eta = 0.1
        bl = 50
        aff = aff_cpp.AFF(alpha, eta, bl)
        
        cp = {}        
        for idx, metric in enumerate(self.metrics_in_view):
            y_series = self.dict_to_list_by_key(time_series, metric)
            x_series = self.dict_to_list_by_key(time_series, x_attr)
            x_min, x_max = np.amin(x_series), np.amax(x_series)
            lin_space = np.linspace(x_min, x_max, len(x_series))
            xy_series = np.interp(lin_space, x_series, y_series)                                                
            
            xy_series_np = np.asarray(xy_series)
            cp[metric] =  aff.process(xy_series_np)[1]            
        return cp

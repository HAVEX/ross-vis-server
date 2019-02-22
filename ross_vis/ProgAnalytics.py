import pandas as pd
import numpy as np
import timeit

# Change point detection methods
from change_point_detection.ffstream.aff_cpp import AFF
from change_point_detection.pca_stream_cpd import pca_stream_cpd_cpp
from ross_vis.prog_inc_pca import ProgIncPCA

from ross_vis.causality import Causality

class PCAStreamCPD(pca_stream_cpd_cpp.PCAStreamCPD):
    def __init__(self,
                 win_size,
                 theta_factor=0.0,
                 divergence_metric="area",
                 thres_total_ex_var_ratio=0.99,
                 delta=0.005,
                 bin_width_factor=2.0):
        super().__init__(win_size, theta_factor, divergence_metric,
                         thres_total_ex_var_ratio, delta, bin_width_factor)

    def feed_predict(self, new_time_point):
        return super().feed_predict(new_time_point)

class StreamData:
    def __init__(self, data, granularity, metric, time_domain):
        self.count = 0
        self.granularity = granularity
        self.time_domain = time_domain
        self.metric = metric
        self.df = pd.DataFrame(data)
        self.time_df = self.preprocess(self.df)
        self.new_time_df = self.time_df
        #if index is not None:
        #    self.df.set_index(index)    
        
    def groupby(self, keys, metric = 'mean'):
        self.groups = self.data.groupby(keys)
        measure = getattr(self.groups, metric)
        self.data = measure()
        return self

    def preprocess(self, curr_df):
        groups = curr_df.groupby([self.granularity, self.time_domain])
        measure = getattr(groups, 'mean')
        data = measure()
        table = pd.pivot_table(data, values=[self.metric], index=[self.granularity], columns=[self.time_domain])
        column_names = []
        for name, group in groups:
            column_names.append(name[1])
        table.columns = [column_names[0]]
        return table

    def update(self, new_data):
        new_data_df = pd.DataFrame(new_data)
        self.df = pd.concat([self.df, new_data_df])  
        self.new_time_df = self.preprocess(new_data_df)
        self.time_df.reset_index(drop=True, inplace=True)
        self.new_time_df.reset_index(drop=True, inplace=True)
        self.time_df = pd.concat([self.time_df, self.new_time_df], axis=1)
        self.count = self.count + 1

    def to_csv(self):
        self.time_df.to_csv('main.csv')


class CPD(StreamData):
    def __init__(self):
        self.cps = []

    def tick(self, data, method):
        ret = False
        self.time_df = data.time_df
        self.method = method
        if(self.method == 'pca_stream'):
            ret = self.pca_stream()
        elif(self.method == 'pca_aff' and self.time_df.shape[1] >= 2):
            ret = self.pca_aff()
        return ret 

    def get_change_points(self):
        return self.cps

    def pca_stream(self):    
        cpd = PCAStreamCPD(win_size=5)
        time_series = self.time_df.T.values
        for i, new_time_point in enumerate(time_series):
            change = cpd.feed_predict(new_time_point)
            if change:
                self.cps.append(i)
        return change

    def pca_aff(self):
        alpha = 0.05
        eta = 0.01
        bl = 5

        # perform PCA to reduce the dimensions
        X = np.array(self.time_df)
        # dft, xt: row: time points (time), col: data points (KPs)
        Xt = X.transpose()
        dft = self.time_df.transpose()
        pca = ProgIncPCA(1)
        pca.progressive_fit(Xt)
        Y = pca.transform(Xt)
        
        # perform adaptive forgetting factor CPD
        aff = AFF(alpha, eta, bl)
        change = np.array(aff.process(Y)[0])
        print(change)
        return change.tolist()[-1]

class PCA(StreamData):
    def __init__(self):
        self.n_components = 2
        self.time_series = np.array([])
        self.pcs_curr = np.array([])
        self.pcs_new = np.array([]) 
        self.pcs_curr_bg = np.array([])

    def format(self):
        pca_result = pd.DataFrame(data = self.pcs_curr, columns = ['PC%d' %x for x in range(0, self.n_components) ])
        schema = {k:type(v).__name__ for k,v in pca_result.items()}
        return({
            'data': pca_result.to_dict('records'),
            'schema': schema
        })

    def tick(self, data, method):
        ret = np.array([])
        self.df = data.df
        self.time_df = data.time_df
        self.new_time_df = data.new_time_df
        self.method = method
        if(self.time_df.shape[1] < 2):
            pass
        elif(self.time_df.shape[1] == 2):
            if(self.method == 'prog_inc'):
                self.prog_inc()
            elif(self.method == 'inc'):
                self.inc()
            return self.format()
        else:
            if(self.method == 'prog_inc'):
                self.prog_inc_update()
            elif(self.method == 'inc'):
                ret = self.inc()
            return self.format()
            
    def prog_inc(self):
        pca = ProgIncPCA(2, 1.0)
        self.time_series = self.time_df.values
        pca.progressive_fit(self.time_series, 10, "random")
        self.pcs_curr = pca.transform(self.time_series) 
        pca.get_loadings()

    def prog_inc_update(self):
        new_time_series = self.new_time_df.values
        self.time_series = np.append(self.time_series, new_time_series, 1)
        pca = ProgIncPCA(2, 1.0)
        pca.progressive_fit(self.time_series, latency_limit_in_msec = 10)
        self.pcs_new = pca.transform(self.time_series)
        #geom_trans_mat = pca.adaptive_progresive_geom_trans_2d(self.pcs_curr, self.pcs_new, latency_limit_in_msec = 10)
        #self.pcs_curr_bg = geom_trans_mat.dot(self.pcs_new.transpose()).transpose()
        self.pcs_curr = self.pcs_new

    def inc(self):
        pca = IncPCA(2, 1.0)
        pca.partial_fit(self.data)
        pcs = pca.transform(self.data.values)
        pca_result = pd.DataFrame(data = pcs, columns = ['PC%d'%x for x in range(0, self.n_components) ])
        return pca_result

    def inc_update(self):
        pass

class Causal(StreamData):
    def __init__(self):
        pass

    def tick(self, data, method):
        self.df = data.df
        metrics = ['NetworkRecv', 'NetworkSend', 'NeventProcessed', 'NeventRb', \
           'RbSec', 'RbTotal', 'VirtualTimeDiff']
        casuality = Causality()
        casuality.adaptive_progresive_var_fit(self.df, latency_limit_in_msec=100)
        casuality_from, casuality_to = casuality.check_causality('RbSec', signif=0.1)
        ir_from, ir_to = casuality.impulse_response('RbSec')
        vd_from, vd_to = casuality.variance_decomp('RbSec')

        print(id_from, vd_from)

        print(pd.DataFrame({
           'Metrics': metrics,
           'Causality': causality_from,
           'IR 1 step later': ir_from[:, 1],
           'VD 1 step later': vd_from[:, 1]
        }))  

class Clustering(StreamData):
    def __init__(self):
        pass

    def tick(self, data, method):
        pass
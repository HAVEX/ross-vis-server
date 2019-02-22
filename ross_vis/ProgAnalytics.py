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
        self.granularity = granularity
        self.time_domain = time_domain
        self.metric = metric
        self.df = pd.DataFrame(data)
        self.time_df = self.preprocess()
        #if index is not None:
        #    self.df.set_index(index)    
        

    def groupby(self, keys, metric = 'mean'):
        self.groups = self.data.groupby(keys)
        measure = getattr(self.groups, metric)
        self.data = measure()
        return self

    def preprocess(self):
        groups = self.df.groupby([self.granularity, self.time_domain])
        measure = getattr(groups, 'mean')
        data = measure()
        table = pd.pivot_table(data, values=[self.metric], index=[self.granularity], columns=[self.time_domain])
        return table

    def update(self, new_data):
        new_data_df = pd.DataFrame(new_data)
        self.df = pd.concat([self.df, new_data_df])  
        self.time_df = self.preprocess()

    def to_csv(self):
        self.time_df.to_csv('main.csv')


class CPD(StreamData):
    def __init__(self):
        pass

    def tick(self, data, method):
        self.time_df = data.time_df
        self.method = method
        if(self.method == 'pca_stream'):
            self.pca_stream()
        elif(self.method == 'pca_aff' and self.time_df.shape[1] >= 2):
            self.pca_aff()

    def pca_stream(self):    
        cpd = PCAStreamCPD(win_size=5)
        time_series = self.time_df.T.values
        pca_cpd_result = []
        for i, new_time_point in enumerate(time_series):
            change = cpd.feed_predict(new_time_point)
            if change:
                pca_cpd_result.append(i)
        return pca_cpd_result

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
        change_points = np.array(aff.process(Y)[1])
        return change_points

class PCA(StreamData):
    def __init__(self):
        pass

    def tick(self, data, method):
        self.df = data.df
        self.time_df = data.time_df
        self.method = method
        if(self.method == 'prog_inc' and self.time_df.shape[1] >= 2):
            self.prog_inc()
        elif(self.method == 'inc' and self.time_df.shape[1] >= 2):
            self.inc()

    def prog_inc(self):
        pass

    def inc(self):
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
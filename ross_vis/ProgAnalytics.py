from ross_vis.DataModel import RossData
from ross_vis.Transform import flatten, flatten_list
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np
import timeit

from ross_vis.causality import Causality

# Dimensionality reduction methods
from dim_reduction.prog_inc_pca import prog_inc_pca_cpp
from dim_reduction.inc_pca import inc_pca_cpp
#from dim_reduction.a_tsne import a_tsne_cpp

# Change point detection methods
from change_point_detection.ffstream.aff_cpp import AFF
from change_point_detection.pca_stream_cpd import pca_stream_cpd_cpp
from ross_vis.prog_inc_pca import ProgIncPCA

class ProgAnalytics:
    def __init__(self, data, granularity, metric, time_domain):
        self.df = pd.DataFrame(data)
        self.time_df = self.preprocess(self.df, granularity, metric, time_domain)
        #if index is not None:
        #    self.df.set_index(index)    

    def groupby(self, keys, metric = 'mean'):
        self.groups = self.data.groupby(keys)
        measure = getattr(self.groups, metric)
        self.data = measure()
        return self

    def preprocess(self, d, granularity, metric, time_domain):
        groups = d.groupby([granularity, time_domain])
        measure = getattr(groups, 'mean')
        data = measure()
        table = pd.pivot_table(data, values=[metric], index=[granularity], columns=[time_domain])
        return table

    def update(self, data, granularity, metric, time_domain):
        in_df = pd.DataFrame(data)
        self.time_df = self.preprocess(self.df, granularity, metric, time_domain)
        self.df = pd.concat([self.df, in_df])  
        self.cpd = self.pca_stream_cpd()
        if(self.time_df.shape[1] >= 2):
            self.aff_cpd = self.pca_aff_cpd()

    def to_csv(self):
        self.time_df.to_csv('main.csv')

    def pca_stream_cpd(self):    
        cpd = PCAStreamCPD(win_size=5)
        time_series = self.time_df.T.values
        pca_cpd_result = []
        for i, new_time_point in enumerate(time_series):
            change = cpd.feed_predict(new_time_point)
            if change:
                pca_cpd_result.append(i)
                print('Change point at {0}'.format(i))
        return pca_cpd_result

    def pca_aff_cpd(self):
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
        print(np.trim_zeros(change_points))

    def prog_inc_pca(self, n_components = 2, forgetting_factor = 1.0, attr='RbSec'):
        pca = prog_inc_pca_cpp.ProgIncPCA(2, 1.0)
        table = pd.pivot_table(self.data, values=[str(attr)], index=['KpGid'], columns=['LastGvt'])
        pca.progressive_fit(table.values, 10, "random")
        pcs = pca.transform(table.values)
        pca.get_loadings()
        pca_result = pd.DataFrame(data = pcs, columns = ['PC%d' %x for x in range(0, n_components) ])
        return pca_result
  
    def inc_pca(self, n_components = 2):
        pca = inc_pca_cpp.IncPCA(2, 1.0)
        pca.partial_fit(self.data)
        pcs = pca.transform(self.data.values)
        pca_result =  pd.DataFrame(data = pcs, columns = ['PC%d'%x for x in range(0, n_components) ])
        return pca_result
    
    def a_tsne(self):
        return tsne_result
    
    def aff_cpd(self):      
        return aff_result

    def causality(self):
        metrics = ['NetworkRecv', 'NetworkSend', 'NeventProcessed', 'NeventRb', \
           'RbSec', 'RbTotal', 'VirtualTimeDiff']
        data = self.data
        casuality = Causality()
        casuality.adaptive_progresive_var_fit(data, latency_limit_in_msec=100)
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

    
  

    def aff_cpd(self):
        return

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
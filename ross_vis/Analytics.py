from ross_vis.DataModel import RossData
from ross_vis.Transform import flatten, flatten_list
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import pandas as pd
import numpy as np

# Dimensionality reduction methods
from dim_reduction.prog_inc_pca import prog_inc_pca_cpp
from dim_reduction.inc_pca import inc_pca_cpp
#from dim_reduction.a_tsne import a_tsne_cpp

# Change point detection methods
from change_point_detection.ffstream import aff_cpp
from change_point_detection.pca_stream_cpd import pca_stream_cpd_cpp

class Analytics:
    def __init__(self, data, index):
        self.data = pd.DataFrame(data)
        if index is not None:
            self.data.set_index(index)
        print(self.data.columns)

    def groupby(self, keys, metric = 'mean'):
        self.groups = self.data.groupby(keys)
        measure = getattr(self.groups, metric)
        self.data = measure()
        return self

    def kmeans(self, k=3):
        kmeans = KMeans(n_clusters=k, random_state=0).fit(self.data.values)
        self.data['kmeans'] = kmeans.labels_
        return kmeans.labels_

    def pca(self, n_components = 2):
        pca = PCA(n_components)
        std_data = StandardScaler().fit_transform(self.data.values)
        pcs = pca.fit_transform(std_data)
        pca_result =  pd.DataFrame(data = pcs, columns = ['PC%d'%x for x in range(0, n_components) ])

        for pc in pca_result.columns.values:
            self.data[pc] = pca_result[pc].values
            # self.data = pd.concat([self.data, pca_result], axis=1, sort=False)
            return pca_result

    def prog_inc_pca(self, n_components = 2, forgetting_factor = 1.0):
        pca = prog_inc_pca_cpp.ProgIncPCA(2, 1.0)
        pca.progressive_fit(self.data.values, 10, "random")
        pcs = pca.transform(self.data.values)
        pca.get_loadings()
        pca_result = pd.DataFrame(data = pcs, columns = ['PC%d' %x for x in range(0, n_components) ])

        for pc in pca_result.columns.values:
            self.data[pc] = pca_result[pc].values
            return pca_result
  
    def inc_pca(self, n_components = 2):
        pca = inc_pca_cpp.IncPCA(2, 1.0)
        pca.partial_fit(self.data.values)
        pcs = pca.transform(self.data.values)
        pca_result =  pd.DataFrame(data = pcs, columns = ['PC%d'%x for x in range(0, n_components) ])

        for pc in pca_result.columns.values:
            self.data[pc] = pca_result[pc].values
            # self.data = pd.concat([self.data, pca_result], axis=1, sort=False)
            return pca_result
    
    def a_tsne(self):
        return tsne_result
    
    def aff_cpd(self):      
        return aff_result

    def pca_stream_cpd_process(self, y_domain):
        '''
            Convert the grouped Dataframe into np.array([p1, p2, p3....],
                                                        [p1, p2, p3....]
                                                        ,...,...,...,...)
        '''
        ret = np.zeros([53,3], dtype=int)
        temp_key = None
        idx = 0
        keys = {}
        for key, item in self.groups:
            if temp_key == None:
                temp_key = key[0]
            if key[0] != temp_key:
                temp_key = key[0]
                idx = idx + 1
                keys[idx] = temp_key
                ret[idx][int(key[1])] = self.groups.get_group(key)[y_domain]
            else:
                ret[idx][int(key[1])] = self.groups.get_group(key)[y_domain]
        return ret, keys

    def pca_stream_cpd(self, y_domain):    
        time_series, groups = self.pca_stream_cpd_process(y_domain)    
        cpd = PCAStreamCPD(win_size=5)
        pca_cpd_result = []
        for i, new_time_point in enumerate(time_series):
            change = cpd.feed_predict(new_time_point)
            if change:
                pca_cpd_result.append(groups[i])
                print('Change point at {0}'.format(groups[i]))
        return pca_cpd_result

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
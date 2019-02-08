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

class Analytics:
    def __init__(self, data, index):
        self.data = pd.DataFrame(data)
        if index is not None:
            self.data.set_index(index)

    def groupby(self, keys, metric = 'mean'):
        groups = self.data.groupby(keys)
        measure = getattr(groups, metric)
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

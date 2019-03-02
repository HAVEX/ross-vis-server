from ross_vis.DataModel import RossData
from ross_vis.Transform import flatten, flatten_list
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import pandas as pd
import numpy as np
from ross_vis.causality import Causality

# Dimensionality reduction methods
from dim_reduction.prog_inc_pca import prog_inc_pca_cpp
from dim_reduction.inc_pca import inc_pca_cpp
#from dim_reduction.a_tsne import a_tsne_cpp

# Change point detection methods
from change_point_detection.ffstream import aff_cpp
from change_point_detection.pca_stream_cpd import pca_stream_cpd_cpp

class Analytics:
    def __init__(self, data, index):
        self.data = pd.DataFrame(data, columns=index)
        if index is not None:
            self.data.set_index(index)

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
        pca_result = pd.DataFrame(data = pcs, columns = ['PC%d'%x for x in range(0, n_components) ])

        for pc in pca_result.columns.values:
            self.data[pc] = pca_result[pc].values
            self.data = pd.concat([self.data, pca_result], axis=1, sort=False)

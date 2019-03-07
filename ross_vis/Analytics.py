from ross_vis.DataModel import RossData
from ross_vis.Transform import flatten, flatten_list
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import pandas as pd
import numpy as np

class Analytics:

    def __init__(self, data, index, excludes = []):
      self.schema = {k:type(v).__name__ for k,v in data[0].items()}
      self.data = pd.DataFrame(data).drop(excludes, axis=1)
      self._result = self.data
      if index is not None:
        self.data.set_index(index)

      for col in excludes:
        del self.schema[col]

    def result(self):
      return self._result

    def groupby(self, keys, metric = 'mean'):
      groups = self.data.groupby(keys)
      measure = getattr(groups, metric)
      self.data = measure()
      new_schema = {}
      for k in self.data.columns.values:
        new_schema[k] = self.schema[k]
      self.schema = new_schema
      return self

    def kmeans(self, k=3):
      kmeans = KMeans(n_clusters=k, random_state=0).fit(self._result)
      self.data['kmeans'] = kmeans.labels_
      self.schema['kmeans'] = 'int'
      # self._result = kmeans.labels_
      return self

    def pca(self, n_components = 2):
      pca = PCA(n_components)
      std_data = StandardScaler().fit_transform(self.data.values)
      pcs = pca.fit_transform(std_data)
      pca_result =  pd.DataFrame(data = pcs, columns = ['PC%d'%x for x in range(0, n_components) ])

      for pc in pca_result.columns.values:
        self.data[pc] = pca_result[pc].values
        self.schema[pc] = 'float'
      # self.data = pd.concat([self.data, pca_result], axis=1, sort=False)
      self._result = pca_result
      return self

    def dbscan(self):
      X = StandardScaler().fit_transform(self._result)
      db = DBSCAN(eps=0.3, min_samples=3).fit(X)
      self.data['dbscan'] = db.labels_
      self.schema['dbscan'] = 'int'
      # self._result = db.labels_
      return self
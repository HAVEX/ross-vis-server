import pandas as pd
import numpy as np
import time
from collections import defaultdict

# Change point detection methods
from change_point_detection.ffstream.aff_cpp import AFF
from change_point_detection.pca_stream_cpd import pca_stream_cpd_cpp
from change_point_detection.pca_aff_cpd import pca_aff_cpd_cpp

# PCA methods
from ross_vis.prog_inc_pca import ProgIncPCA
#from ross_vis.inc_pca import IncPCA

# Clustering methods
from ross_vis.prog_evo_stream import ProgEvoStream
from ross_vis.prog_kmeans import ProgKMeans

# Causality methods
from ross_vis.causality import Causality

#from timer import Timer


class PCAAFFCPD(pca_aff_cpd_cpp.PCAAFFCPD):
    def __init__(self,
                 alpha,
                 eta=0.01,
                 burn_in_length=10,
                 inc_pca_forgetting_factor=1.0):
        super().__init__(alpha, eta, burn_in_length, inc_pca_forgetting_factor)

    def feed(self, new_time_point):
        return super().feed(new_time_point)

    def feed_with_pca_result_return(self, new_time_point):
        return super().feed_with_pca_result_return(new_time_point)

    def predict(self):
        return super().predict()

    def feed_predict(self, new_time_point):
        return super().feed_predict(new_time_point)


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
        self.new_data_df = self.df
        self.metric_df = self.preprocess(self.df)
        # set new_data_df for the first stream as metric_df
        self.whole_data_df = self.metric_df

        self.algo_clustering = 'kmeans'

        self.cpd = CPD()
        self.pca = PCA()
        self.causal = Causal()
        self.clustering = Clustering()
        self.results = pd.DataFrame(data=self.df[granularity].astype(np.float64).tolist(), columns=[granularity])
        self._time = self.metric_df.columns.get_level_values(1).tolist()
        self.granIDs = self.df[self.granularity]
        #self.timer = Timer()


    def _format(self):
        # Convert metric_df to ts : { id1: [timeSeries], id2: [timeSeries] }    
        ret = {}
        columns = self.metric_df.columns.get_level_values(level=self.time_domain).tolist()
        ids = self.metric_df.T.columns.tolist()
        for i in ids:
            ret[i] = self.metric_df.T[(i)].tolist()
        ret_df = pd.DataFrame.from_dict(ret)
        schema = {k:type(v).__name__ for k,v in ret_df[0].items()}
        return({
            'data': ret,
            'schema': schema
        })

    def process_type(self, type):
        if(type == 'int64'):
            return 'int'
        if(type == 'float64'):
            return 'float'
        if(type == 'list'):
            return 'int'

    def format(self):
       #print(self.timer)
        schema = {k:self.process_type(type(v).__name__) for k,v in self.df.iloc[0].items()}
        return  (self.results.to_dict('records'), schema)

    def comm_data(self):
        columns = ['CommData', 'RbTotal', 'RbSec', 'Kpid', 'Peid', 'LastGvt']
        _df = self.df[columns]
        _time = self.df[self.time_domain].unique()[self.count]
        _schema = {k:self.process_type(type(v).__name__) for k,v in _df.iloc[0].items()}
        return (_df.to_dict('records'), _time, _schema)

    def groupby(self, df, keys, metric = 'mean'):
        # Groups data by the keys provided
        self.groups = df.groupby(keys)
        measure = getattr(self.groups, metric)
        self.data = measure() 

    def preprocess(self, df):
        # Group the data by granularity (PE, KP, LP) and time. 
        # Converts into a table and the shape is (number of processing elements, number of time steps)
        self.groupby(df, [self.granularity, self.time_domain])
        table = pd.pivot_table(df, values=[self.metric], index=[self.granularity], columns=[self.time_domain])
        self.current_time = table.columns
        return table

    def processByMetric(self, df, metric):
        self.groupby(df, [self.granularity, self.time_domain])
        table = pd.pivot_table(df, values=[metric], index=[self.granularity], columns=[self.time_domain], fill_value=0)
        column_names = []
        for name, group in self.groups:
            column_names.append(name[1])
        table.columns = list(set(column_names))
        return table

    def drop_prev_results(self, attrs): 
        self.results.drop(attrs, axis=1, inplace=True)

    def update(self, new_data):
        self.whole_data_df = pd.DataFrame(new_data)
        self.df = pd.concat([self.df, self.whole_data_df]) 
        self.new_data_df = self.preprocess(self.whole_data_df)
        # To avoid Nan values while concat
        self.metric_df.reset_index(drop=True, inplace=True)
        self.new_data_df.reset_index(drop=True, inplace=True)
        self.metric_df = pd.concat([self.metric_df, self.new_data_df], axis=1).T.drop_duplicates().T
        self.count = self.count + 1
        self._time = self.metric_df.columns.get_level_values(1).tolist()
        self.granIDs = self.df[self.granularity]

        return self     

    def deupdate(self, remove_data):
        self.whole_data_df = pd.DataFrame(remove_data)
        this_time = self.whole_data_df[self.time_domain].unique()[0]
        self.df = self.df[self.df[self.time_domain] != this_time]
        #self.metric_df.drop(columns=[this_time])
        self.count = self.count - 1
        self._time = this_time
        self.granIDs = self.df[self.granularity]  
        return self

    def clean_up(self):
        if(self.count > 2):
            self.drop_prev_results(['cpd'])
            self.drop_prev_results(['from_metrics','from_causality','from_IR_1', 'from_VD_1',
                                   'to_metrics', 'to_causality', 'to_IR_1', 'to_VD_1'
            ])
            self.drop_prev_results(['PC0','PC1'])
            if(self.algo_clustering == 'evostream'):
                self.drop_prev_results(['ids', 'normal', 'normal_clusters', 'normal_times','micro', 'micro_clusters', 'macro', 'macro_clusters', 'macro_times', 'micro_times'])
            elif(self.algo_clustering == 'kmeans'):
                self.drop_prev_results(['ids', 'normal', 'normal_clusters', 'normal_times', 'macro', 'macro_clusters', 'macro_times',])

    def run_methods(self, data, algo):
        self.clean_up()
        clustering_result = self.clustering.tick(data)
        pca_result = self.pca.tick(data, algo['pca'])
        cpd_result = self.cpd.tick(data, algo['cpd'])
        causal_result = self.causal.tick(data, algo['causality'])
        
        if(self.count >= 2):
            self.results = self.results.join(clustering_result)
            self.results = self.results.join(pca_result)
            self.results = self.results.join(cpd_result)
            self.results = self.results.join(causal_result)
        self.results = self.results.fillna(0)   
        #self.to_csv(self.count)
        return self.format()

    def to_csv(self, filename):
        # Write the metric_df to a csv file
        self.results.to_csv(str(filename) + str(self.metric) + '.csv')

    def from_csv(self, filename):
        self.results = pd.read_csv(str(filename) + str(self.metric) + '.csv')
        return self.format()

class CPD(StreamData):
    # Perform Change point detection on Streaming data.
    def __init__(self):
        # Stores the change points recorded.
        self.cps = []
        self.alpha = 0.2
        self.aff_obj = PCAAFFCPD(alpha=self.alpha)

    def format(self, result):
        cpd = [(result)]
        cpd_result = pd.DataFrame(data=cpd, columns=['cpd'],)
        return [cpd_result]

    def tick(self, data, method):
        ret = False
        self.new_data_df = data.new_data_df
        self.count = data.count
        self.metric = data.metric
        self.current_time = data.current_time
        self.method = method
        if(self.count == 1):
            if(self.method == 'aff'):
                result = self.aff()
            elif(self.method == 'stream'):
                result = self.stream()
        else:
            if(self.method == 'aff'):
                result = self.aff_update()
            elif(self.method == 'stream'):
                result = self.stream_update()
        return self.format(result)

    def get_change_points(self):
        # Getter to return the change points.
        return self.cps

    def stream(self):    
        self.stream = PCAStreamCPD(win_size=5)
        time_series = self.new_data_df.T.values
        change = self.stream.feed_predict(new_time_point)
        if change:
            self.cps.append(0)
            print('Change', 0)
            return 1
        else:
            return 0

    def stream_update(self):
        X = np.array(self.new_data_df)
        Xt = X.transpose()
        change = self.stream.feed_predict(Xt)
        if(change):
            self.cps.append(self.count)
            print('Change', self.count)
            return 1
        else:
            return 0

    def aff(self):
        X = np.array(self.new_data_df)
        Xt = X.transpose()
        
        # perform adaptive forgetting factor CPD
        change = self.aff_obj.feed_predict(Xt[0, :])
        if change:
            self.cps.append(0)
            print('#####################Change#######################', 0)
            return 1
        else:
            return 0
            
    def aff_update(self):
        X = np.array(self.new_data_df[self.current_time])
        Xt = X.transpose()
        change = self.aff_obj.feed_predict(Xt[0, :])
        if(change):
            self.cps.append(self.count)
            print('#####################Change#######################', self.count)
            return 1
        else:
            print('#####################No-change#######################', self.count)
            return 0

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

    def _format(self):
        return pd.DataFrame(data = self.pcs_curr, columns = ['PC%d' %x for x in range(0, self.n_components)])

    def tick(self, data, method):
        self.metric_df = data.metric_df
        self.new_data_df = data.new_data_df
        self.method = method
        self.count = data.count

        if(self.count < 2):
            pass
        elif(self.count == 2):
            if(method == 'prog_inc'):
                self.prog_inc()
            elif(self.method == 'inc'):
                self.inc()
            return self._format()
        else:
            if(self.method == 'prog_inc'):
                self.prog_inc_update()
            elif(self.method == 'inc'):
                self.inc()
            return self._format()
            
    def prog_inc(self):
        pca = ProgIncPCA(2, 1.0)
        self.time_series = self.metric_df.values
        pca.progressive_fit(self.time_series, 10, "random")
        self.pcs_curr = pca.transform(self.time_series) 
        pca.get_loadings()

    def prog_inc_update(self):
        new_time_series = self.new_data_df.values
        self.time_series = np.append(self.time_series, new_time_series, 1)
        pca = ProgIncPCA(2, 1.0)
        pca.progressive_fit(self.time_series, latency_limit_in_msec = 10)
        self.pcs_new = pca.transform(self.time_series)
        self.pcs_curr = ProgIncPCA.geom_trans(self.pcs_curr, self.pcs_new)
        
    def inc(self):
        pca = IncPCA(2, 1.0)
        pca.partial_fit(self.data)
        pcs = pca.transform(self.data.values)
        pca_result = pd.DataFrame(data = pcs, columns = ['PC%d'%x for x in range(0, self.n_components) ])
        return pca_result

    def inc_update(self):
        pass

class Clustering(StreamData):
    def __init__(self):
        self.n_clusters = 3
        self.mutation_rate = 0.1
        self.fit_latency_limit_in_msec = 10
        self.refine_latency_limit_in_msec = 30
        self.labels = np.array([])
        self.labels_macro = np.array([])
        self.labels_micro = np.array([])
        self.times_macro = np.array([])
        self.times_micro = np.array([])


    # def format(self):
    #     normal_result = pd.DataFrame.from_dict({'normal' : np.asmatrix(self.time_series).tolist(), 'normal_labels': self.labels }, orient='index')
    #     micro_result = pd.DataFrame.from_dict({'micro' : np.asmatrix(self.time_series_micro).tolist(), 'micro_labels': self.labels_micro }, orient='index')
    #     macro_result = pd.DataFrame.from_dict({'macro' : np.asmatrix(self.time_series_macro).tolist(), 'macro_labels': self.labels_macro }, orient='index')
        
    #     return({
    #         'normal': normal_result,
    #         'micro': micro_result,
    #     })

    def _format(self):
        normal = [(self.time_series.tolist(), self.labels, self._time, self.granIDs.tolist())]
        micro = [(self.time_series_micro.tolist(), self.labels_micro, self._time)]
        macro = [(self.time_series_macro.tolist(), self.labels_macro, self._time)]
        normal_result = pd.DataFrame(data=normal, columns=['normal', 'normal_clusters', 'normal_times', 'ids'])
        micro_result = pd.DataFrame(data=micro, columns=['micro', 'micro_clusters', 'micro_times'])
        macro_result = pd.DataFrame(data=macro, columns=['macro', 'macro_clusters', 'macro_times'])
        return [normal_result, micro_result, macro_result]
        
    def _kmeans_format(self):
        normal = [(self.time_series.tolist(), self.labels, self._time, self.granIDs.tolist())]
        macro = [(self.time_series_macro.tolist(), self.labels_macro, self._time)]
        normal_result = pd.DataFrame(data=normal, columns=['normal', 'normal_clusters', 'normal_times', 'ids'])
        macro_result = pd.DataFrame(data=macro, columns=['macro', 'macro_clusters', 'macro_times'])
        return [normal_result, macro_result]

    def tick(self, data):
        self.metric_df = data.metric_df
        self.new_data_df = data.new_data_df
        self.algo = data.algo_clustering
        self._time = data._time
        self.granIDs = data.granIDs
        self.granularity = data.granularity
        self.count = data.count 
        
        if(self.algo == 'evostream'):
            if(self.count < 2):
                return {}
            if(self.count == 2):
                self.evostream()
            elif(self.count > 2):
                self.evostream_update()
            self.macro()
            self.micro()
            return self._format()
        elif(self.algo == 'kmeans'):
            if(self.count < 2):
                return {}
            if(self.count == 2):
                self.kmeans()
            elif(self.count > 2):
                self.kmeans_update()
            self.kmeans_macro()
            # self.micro()
            return self._kmeans_format()

    def emptyCurrentToPrev(self):
        ret = {}
        for idx in range(self.n_clusters):
            ret[idx] = 0
        return ret

    def evostream(self):
        self.time_series = self.metric_df.values
        self.evo = ProgEvoStream(n_clusters=self.n_clusters, mutation_rate=self.mutation_rate)
        self.evo.progressive_fit(self.time_series, latency_limit_in_msec=self.fit_latency_limit_in_msec)
        self.evo.progressive_refine_cluster(latency_limit_in_msec=self.refine_latency_limit_in_msec)
        self.labels = self.evo.predict(self.time_series)
        self.current_to_prev = self.emptyCurrentToPrev()
        
    def evostream_update(self):
        new_time_series = self.new_data_df.values
        self.time_series = np.append(self.time_series, new_time_series, 1)
        self.evo.progressive_fit(self.time_series, latency_limit_in_msec=self.fit_latency_limit_in_msec, point_choice_method="random", verbose=True)
        self.evo.progressive_refine_cluster(latency_limit_in_msec=self.refine_latency_limit_in_msec)
        self.labels, self.current_to_prev = self.evo.consistent_labels(self.labels, self.evo.predict(self.time_series))

    def macro(self):
        self.time_series_macro = np.array(self.evo.get_macro_clusters())
        self.labels_macro = [self.current_to_prev[i] for i in range(self.time_series_macro.shape[0])]
        self.times_macro = np.array(self._time)

    def micro(self):
        self.time_series_micro = np.array(self.evo.get_micro_clusters())
        self.lables_micro = self.evo.predict(self.time_series_micro)
        self.labels_micro = [self.current_to_prev[i] for i in self.labels_micro]
        self.times_micro = np.array(self._time)

    def kmeans(self):
        self.time_series = self.metric_df.values
        self.evo = ProgKMeans(n_clusters=self.n_clusters)
        self.evo.progressive_fit(self.time_series, latency_limit_in_msec=self.fit_latency_limit_in_msec)
        # self.evo.progressive_refine_cluster(latency_limit_in_msec=self.refine_latency_limit_in_msec)
        self.labels = self.evo.predict(self.time_series).tolist()
        print(self.labels)
        self.current_to_prev = self.emptyCurrentToPrev()
        
    def kmeans_update(self):
        new_time_series = self.new_data_df.values
        self.time_series = np.append(self.time_series, new_time_series, 1)
        self.evo.progressive_fit(self.time_series, latency_limit_in_msec=self.fit_latency_limit_in_msec, point_choice_method="fromPrevCluster", verbose=True)
        # self.evo.progressive_refine_cluster(latency_limit_in_msec=self.refine_latency_limit_in_msec)
        self.labels, self.current_to_prev = self.evo.consistent_labels(self.labels, self.evo.predict(self.time_series))
        print(self.labels)

    def kmeans_macro(self):
        self.time_series_macro = np.array(self.evo.get_centers())
        self.labels_macro = [self.current_to_prev[i] for i in range(self.time_series_macro.shape[0])]
        self.times_macro = np.array(self._time)

    # def micro(self):
    #     self.time_series_micro = np.array(self.evo.get_micro_clusters())
    #     self.lables_micro = self.evo.predict(self.time_series_micro)
    #     self.labels_micro = [self.current_to_prev[i] for i in self.labels_micro]
    #     self.times_micro = np.array(self._time)



class Causal(StreamData):
    def __init__(self):
        self.pivot_table_results = {}

    def numpybool_to_bool(self, arr):
        ret = []
        for idx, val in enumerate(arr):
            if(val == True):
                ret.append(1)
            elif(val == False):
                ret.append(0)
            else:
                ret.append(-1)
        return ret

    def flatten(self, l):
        flat_list = []
        for sublist in l:
            for item in sublist:
                flat_list.append(item)
        return flat_list


    def tick(self, data, method):
        self.df = data.whole_data_df
        self.metric = data.metric
        
        metrics = ['NetworkRecv', 'NetworkSend', 'NeventProcessed', 'NeventRb', 'RbSec', \
         'RbTime', 'RbTotal', 'LastGvt', 'KpGid']

        calc_metrics = ['NetworkRecv', 'NetworkSend', 'NeventRb', 'NeventProcessed', \
           'RbSec']

        pca = ProgIncPCA(1)
        total_latency_for_pca = 100
        latency_for_each = int(total_latency_for_pca / len(metrics))
        n = 128
        X = np.empty(shape=(n, len(metrics)))
        self.df = self.df[metrics] 

        for i, metric in enumerate(calc_metrics):
            start = time.time()
            metric_pd = data.processByMetric(self.df, metric)
            if(metric not in self.pivot_table_results):
                self.pivot_table_results[metric] = metric_pd
            else:
                self.pivot_table_results[metric] = pd.concat([self.pivot_table_results[metric], metric_pd], axis=1)
                metric_pd = self.pivot_table_results[metric]
            metric_nd = metric_pd.values
            
            pca.progressive_fit(
                    metric_nd,
                    latency_limit_in_msec=latency_for_each,
                    point_choice_method='random',
                    verbose=True)
            metric_1d = pca.transform(metric_nd)
            X[:, i] = metric_1d[:, 0]

        X = pd.DataFrame(X, columns=metrics)
        X = X[calc_metrics]
        is_non_const_col = (X != X.iloc[0]).any()
        X = X.loc[:, is_non_const_col]
        X = X.replace([np.inf, -np.inf], np.nan)
        X = X.fillna(0.0)

        causality_from = pd.DataFrame(
            index=[0], columns=calc_metrics).fillna(False)
        causality_to = pd.DataFrame(
            index=[0], columns=calc_metrics).fillna(False)

        ir_from = pd.DataFrame(index=[0], columns=calc_metrics).fillna(0.0)
        ir_to = pd.DataFrame(index=[0], columns=calc_metrics).fillna(0.0)

        vd_from = pd.DataFrame(index=[0], columns=calc_metrics).fillna(0.0)
        vd_to = pd.DataFrame(index=[0], columns=calc_metrics).fillna(0.0)

        if is_non_const_col.loc[data.metric]:
            causality = Causality()
            causality.adaptive_progresive_var_fit(
                X, latency_limit_in_msec=100, point_choice_method="reverse")

            causality_from, causality_to = causality.check_causality(data.metric, signif=0.1)


            try:
                tmp_ir_from, tmp_ir_to = causality.impulse_response(
                    self.metric)
                ir_from.loc[0, is_non_const_col] = tmp_ir_from[:, 1]
                ir_to.loc[0, is_non_const_col] = tmp_ir_to[:, 1]
            except:
                print(
                    "impulse reseponse was not excuted. probably matrix is not",
                    "positive definite")

            try:
                tmp_vd_from, tmp_vd_to = causality.variance_decomp(self.metric)
                vd_from.loc[0, is_non_const_col] = tmp_vd_from[:, 1]
                vd_to.loc[0, is_non_const_col] = tmp_vd_to[:, 1]
            except:
                print(
                    "impulse reseponse was not excuted. probably matrix is not",
                     "positive definite")

        causality_from = causality_from
        causality_to = causality_to
        ir_from = ir_from.loc[0, :].tolist()
        ir_to = ir_to.loc[0, :].tolist()
        vd_from = vd_from.loc[0, :].tolist()
        vd_to = vd_to.loc[0, :].tolist()

        
        from_ = [(calc_metrics, self.numpybool_to_bool(causality_from),
                  ir_from, vd_from)]
        to_ = [(calc_metrics, self.numpybool_to_bool(causality_to), ir_to,
                vd_to)]

        from_result = pd.DataFrame(
            data=from_,
            columns=[
                'from_metrics', 'from_causality', 'from_IR_1', 'from_VD_1'
            ])
        to_result = pd.DataFrame(
            data=to_,
            columns=['to_metrics', 'to_causality', 'to_IR_1', 'to_VD_1'])

        return [from_result, to_result]
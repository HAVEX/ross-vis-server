import pandas as pd
import numpy as np
import timeit
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

# Causality methods
from ross_vis.causality import Causality

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
        self.metric_df = self.preprocess(self.df)
        # set new_data_df for the first stream as metric_df
        self.new_data_df = self.metric_df
        self.cpd = CPD()
        self.pca = PCA()
        self.causal = Causal()
        self.clustering = Clustering()
        self.results = pd.DataFrame(data=self.df[granularity].astype(np.float64).tolist(), columns=[granularity])
        self._time = self.metric_df.columns.get_level_values(1).tolist()
        self.granIDs = self.df[self.granularity]

        

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
        schema = {k:self.process_type(type(v).__name__) for k,v in self.df.iloc[0].items()}
        return (self.df.to_dict('records'), self.results.to_dict('records'), schema)

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
        table = pd.pivot_table(df, values=[metric], index=[self.granularity], columns=[self.time_domain])
        column_names = []
        for name, group in self.groups:
            column_names.append(name[1])
        table.columns = list(set(column_names))
        return table

    def drop_prev_results(self, attrs): 
        self.results.drop(attrs, axis=1, inplace=True)

    def update(self, new_data):
        new_data_df = pd.DataFrame(new_data)
        self.df = pd.concat([self.df, new_data_df]) 
        self.new_data_df = self.preprocess(new_data_df)
        # To avoid Nan values while concat
        self.metric_df.reset_index(drop=True, inplace=True)
        self.new_data_df.reset_index(drop=True, inplace=True)
        self.metric_df = pd.concat([self.metric_df, self.new_data_df], axis=1).T.drop_duplicates().T
        self.count = self.count + 1
        self._time = self.metric_df.columns.get_level_values(1).tolist()
        self.granIDs = self.df[self.granularity]

        return self     

    def clean_up(self):
        if(self.count > 2):
            self.drop_prev_results(['PC0','PC1'])
            self.drop_prev_results(['cpd'])
            self.drop_prev_results(['from_metrics','from_causality','from_IR_1', 'from_VD_1',
                                    'to_metrics', 'to_causality', 'to_IR_1', 'to_VD_1'
            ])
        if(self.count > 3):
            self.drop_prev_results(['ids', 'normal', 'normal_clusters', 'normal_times','micro', 'micro_clusters', 'macro', 'macro_clusters', 'macro_times', 'micro_times'])

    def run_methods(self, data, algo):
        self.clean_up()
        clustering_result = self.clustering.tick(data)
        pca_result = self.pca.tick(data, algo['pca'])
        cpd_result = self.cpd.tick(data, algo['cpd'])
        causal_result = self.causal.tick(data, algo['causality'])
        
        if(self.count > 2):
            self.results = self.results.join(clustering_result)
        self.results = self.results.join(cpd_result)
        self.results = self.results.join(pca_result)
        self.results = self.results.join(causal_result)
        self.results = self.results.fillna(0)   
        return self.format()

    def to_csv(self):
        # Write the metric_df to a csv file
        self.df.to_csv(self.metric + '.csv')


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
            print('Change', 0)
            return 1
        else:
            return 0
            
    def aff_update(self):
        X = np.array(self.new_data_df[self.current_time])
        Xt = X.transpose()
        change = self.aff_obj.feed_predict(Xt[0, :])
        if(change):
            self.cps.append(self.count)
            print('Change', self.count)
            return 1
        else:
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
        geom_trans_mat = pca.adaptive_progresive_geom_trans_2d(self.pcs_curr, self.pcs_new, latency_limit_in_msec = 10)
        self.pcs_curr_bg = geom_trans_mat.dot(self.pcs_new.transpose()).transpose()
        self.pcs_curr = self.pcs_curr_bg

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


    def format(self):
        normal_result = pd.DataFrame.from_dict({'normal' : np.asmatrix(self.time_series).tolist(), 'normal_labels': self.labels }, orient='index')
        micro_result = pd.DataFrame.from_dict({'micro' : np.asmatrix(self.time_series_micro).tolist(), 'micro_labels': self.labels_micro }, orient='index')
        macro_result = pd.DataFrame.from_dict({'macro' : np.asmatrix(self.time_series_macro).tolist(), 'macro_labels': self.labels_macro }, orient='index')
        
        return({
            'normal': normal_result,
            'micro': micro_result,
        })

    def _format(self):
        normal = [(self.time_series.tolist(), self.labels, self._time, self.granIDs.tolist())]
        micro = [(self.time_series_micro.tolist(), self.labels_micro, self._time)]
        macro = [(self.time_series_macro.tolist(), self.labels_macro, self._time)]
        normal_result = pd.DataFrame(data=normal, columns=['normal', 'normal_clusters', 'normal_times', 'ids'])
        micro_result = pd.DataFrame(data=micro, columns=['micro', 'micro_clusters', 'micro_times'])
        macro_result = pd.DataFrame(data=macro, columns=['macro', 'macro_clusters', 'macro_times'])
        return [normal_result, micro_result, macro_result]

    def tick(self, data):
        self.metric_df = data.metric_df
        self.new_data_df = data.new_data_df
        self._time = data._time
        self.granIDs = data.granIDs
        self.granularity = data.granularity
        self.count = data.count 
        
        if(self.count < 2):
            return {}

        if(self.count == 2):
            self.evostream()
        elif(self.count > 2):
            self.evostream_update()
            self.macro()
            self.micro()
            return self._format()

    def evostream(self):
        self.time_series = self.metric_df.values
        self.evo = ProgEvoStream(n_clusters=self.n_clusters, mutation_rate=self.mutation_rate)
        self.evo.progressive_fit(self.time_series, latency_limit_in_msec=self.fit_latency_limit_in_msec)
        self.evo.progressive_refine_cluster(latency_limit_in_msec=self.refine_latency_limit_in_msec)
        self.labels = self.evo.predict(self.time_series)
        
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

class Causal(StreamData):
    def __init__(self):
        pass

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

    def tick(self, data, method):
        self.df = data.df
        self.metric = data.metric
        metrics = ['NetworkRecv', 'NetworkSend', 'NeventProcessed', 'NeventRb', \
           'RbSec', 'RbTotal', 'VirtualTimeDiff', 'KpGid', 'LastGvt', 'Peid', 'RealTs', 'VirtualTs']

        calc_metrics = ['NetworkRecv', 'NetworkSend', 'NeventRb', 'NeventProcessed', \
           'RbSec', 'RbTotal', 'VirtualTimeDiff']

        pca = ProgIncPCA(1)
        total_latency_for_pca = 100
        latency_for_each = int(total_latency_for_pca/len(metrics))
        X_dict = {}
        self.df = self.df[metrics]

        for metric in calc_metrics:
            metric_nd = data.processByMetric(self.df, metric).values
            pca.progressive_fit(metric_nd, latency_limit_in_msec=latency_for_each, point_choice_method='random', verbose=True)
            metric_ld = pca.transform(metric_nd)
            X_dict[metric] = metric_ld.flatten().tolist()
        X = pd.DataFrame(X_dict)
        X = X.loc[:, (X != X.iloc[0]).any()]


        causality = Causality()
        causality.adaptive_progresive_var_fit(X, latency_limit_in_msec=100, point_choice_method="reverse")
        causality_from, causality_to = causality.check_causality(data.metric, signif=0.1)
        ir_from, ir_to = causality.impulse_response(self.metric)
        vd_from, vd_to = causality.variance_decomp(self.metric)

        from_ = [(calc_metrics, self.numpybool_to_bool(causality_from), ir_from[:, 1].tolist(), vd_from[:, 1].tolist())]
        to_ = [(calc_metrics, self.numpybool_to_bool(causality_to), ir_to[:, 1].tolist(), vd_to[:, 1].tolist())]

        from_result = pd.DataFrame(data=from_, columns=['from_metrics','from_causality','from_IR_1', 'from_VD_1'])
        to_result = pd.DataFrame(data=to_, columns=['to_metrics','to_causality','to_IR_1', 'to_VD_1'])
    
        return [from_result, to_result]



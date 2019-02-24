import pandas as pd
import numpy as np
import timeit

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

    def format(self):
        # Convert metric_df to ts : { id1: [timeSeries], id2: [timeSeries] }
        ret = {}
        for idx, row in self.metric_df.iterrows():
            values = row.tolist()
            if idx not in ret:
                ret[idx] = []
            ret[idx].append(values)
        
        return({
            'ts': ret,
        })

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
        column_names = []
        for name, group in self.groups:
            column_names.append(name[1])
        table.columns = [column_names[0]]
        return table

    def update(self, new_data):
        new_data_df = pd.DataFrame(new_data)
        self.df = pd.concat([self.df, new_data_df])  
        self.new_data_df = self.preprocess(new_data_df)
        # To avoid Nan values while concat
        self.metric_df.reset_index(drop=True, inplace=True)
        self.new_data_df.reset_index(drop=True, inplace=True)
        self.metric_df = pd.concat([self.metric_df, self.new_data_df], axis=1)
        self.count = self.count + 1
        return self.format()

    def to_csv(self):
        # Write the metric_df to a csv file
        self.metric_df.to_csv(self.metric + '.csv')


class CPD(StreamData):
    # Perform Change point detection on Streaming data.
    def __init__(self):
        # Stores the change points recorded.
        self.cps = []

    def tick(self, data, method):
        ret = False
        self.new_data_df = data.new_data_df
        self.count = data.count
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
        return result

    def get_change_points(self):
        # Getter to return the change points.
        return self.cps

    def stream(self):    
        self.stream = PCAStreamCPD(win_size=5)
        time_series = self.new_data_df.T.values
        change = self.stream.feed_predict(new_time_point)
        if change:
            self.cps.append(0)
            print('change', 0)
            return True
        else:
            return False

    def stream_update(self):
        X = np.array(self.new_data_df)
        Xt = X.transpose()
        change = self.stream.feed_predict(Xt)
        if(change):
            self.cps.append(self.count)
            print('Change', self.count)
            return True
        else:
            return False

    def aff(self):
        alpha = 0.2
        X = np.array(self.new_data_df)
        Xt = X.transpose()
        
        # perform adaptive forgetting factor CPD
        self.aff = PCAAFFCPD(alpha=alpha)
        change = self.aff.feed_predict(Xt[0, :])
        if change:
            self.cps.append(0)
            print('change', 0)
            return True
        else:
            return False
            
    def aff_update(self):
        X = np.array(self.new_data_df)
        Xt = X.transpose()
        change = self.aff.feed_predict(Xt)
        if(change):
            self.cps.append(self.count)
            print('Change', self.count)
            return True
        else:
            return False

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
            return self.format()
        else:
            if(self.method == 'prog_inc'):
                self.prog_inc_update()
            elif(self.method == 'inc'):
                self.inc()
            return self.format()
            
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


class Clustering(StreamData):
    def __init__(self):
        self.n_clusters = 3
        self.mutation_rate = 0.1
        self.fit_latency_limit_in_msec = 10
        self.refine_latency_limit_in_msec = 30
        self.labels = np.array([])
        self.labels_macro = np.array([])
        self.labels_micro = np.array([])


    def format(self):
        normal_result = pd.DataFrame.from_dict({'data' : np.asmatrix(self.time_series).tolist(), 'clusters': self.labels }, orient='index')
        micro_result = pd.DataFrame.from_dict({'data' : np.asmatrix(self.time_series_micro).tolist(), 'clusters': self.labels_micro }, orient='index')
        macro_result = pd.DataFrame.from_dict({'data' : np.asmatrix(self.time_series_macro).tolist(), 'clusters': self.labels_macro }, orient='index')
        
        schema = {k:type(v).__name__ for k,v in normal_result.items()}
        return({
            'normal': normal_result.to_dict('records'),
            'micro': micro_result.to_dict('records'),
            'macro': macro_result.to_dict('records'),
            'schema': schema
        })

    def tick(self, data):
        self.metric_df = data.metric_df
        self.new_data_df = data.new_data_df
        self.count = data.count 
        
        if(self.count < 2):
            return

        if(self.count == 2):
            self.evostream()
            return
        elif(self.count > 2):
            self.evostream_update()
            self.macro()
            self.micro()
            return self.format()

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
        #self.labels, self.current_to_prev = ProgEvoStream.consistent_labels(self.labels, self.evo.predict(self.time_series))
        self.labels, self.current_to_prev = self.labels, self.evo.predict(self.time_series)

    def macro(self):
        self.time_series_macro = np.array(self.evo.get_macro_clusters())
        self.labels_macro = [self.current_to_prev[i] for i in range(self.time_series_macro.shape[0])]

    def micro(self):
        self.time_series_micro = np.array(self.evo.get_micro_clusters())
        self.lables_micro = self.evo.predict(self.time_series_micro)
        self.labels_micro = [self.current_to_prev[i] for i in self.labels_micro]

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

import time
import math
import numpy as np
from random import shuffle
from statsmodels.tsa.vector_ar.var_model import VAR


class Causality():
    """Causality analysis with vector autoregression, Granger causality test,
    impulse response function, and variance decompositon. Additionally,
    progressive usage of vector autoregression is included.
    Implementation is based on 'statsmodels' package
    (https://github.com/statsmodels/statsmodels).
    statsmodels' vector autoregression includes automatic selection of best
    lags and orders.
    Parameters
    ----------
    Attributes
    ----------
    var_result: VARResults object
        The result obtained with var_fit or adaptive_progresive_var_fit.
    n_processed_in_prev_var_fit: int
        A number of processed time points in a previous call of
        adaptive_progresive_var_fit.
    duration_in_prev_var_fit: float
        Completion time spent in a previous call of adaptive_progresive_var_fit.
    Examples
    --------
    >>> import numpy as np
    >>> import pandas as pd
    >>> from causality import Causality

    >>> # NEED TO LOAD "prog_inc_pca" for handling 2. multiple data points with PCA
    >>> from prog_inc_pca import ProgIncPCA

    >>> #
    >>> # 1. signle data point example
    >>> #
    >>> # prepare data
    >>> X = pd.read_csv('./data/ross-df-256kp-500gvt_kpgid100.csv')
    >>> # only take useful columns
    >>> metrics = [
    ...     'NetworkRecv', 'NetworkSend', 'NeventProcessed', 'NeventRb', 'RbSec',
    ...     'RbTotal', 'VirtualTimeDiff'
    ... ]
    >>> X = X[metrics]

    >>> # causality analysis
    >>> causality = Causality()
    >>> # VAR with the progressive way
    >>> causality.adaptive_progresive_var_fit(X, latency_limit_in_msec=100)
    >>> # Granger causality test from others to RbSec and RbSec to others
    >>> causality_from, causality_to = causality.check_causality('RbSec', signif=0.1)
    >>> # impulse response and variance decomposition
    >>> ir_from, ir_to = causality.impulse_response('RbSec')
    >>> vd_from, vd_to = causality.variance_decomp('RbSec')

    >>> # print results
    >>> print('KpGid = 100')
    >>> print('Rbsec (caused by)')
    >>> print(
    ...     pd.DataFrame({
    ...         'Metrics': metrics,
    ...         'Causality': causality_from,
    ...         'IR 1 step later': ir_from[:, 1],
    ...         'VD 1 step later': vd_from[:, 1]
    ...     }))
    >>> print('Rbsec (causing to)')
    >>> print(
    ...     pd.DataFrame({
    ...         'Metrics': metrics,
    ...         'Causality': causality_to,
    ...         'IR 1 step later': ir_to[:, 1],
    ...         'VD 1 step later': vd_to[:, 1]
    ...     }))

    >>> #
    >>> # 2. multiple data points with PCA
    >>> #
    >>> # apply progressive PCA for each metric to reduce dimensions (multiple time-series) to 1D (single time-series)
    >>> pca = ProgIncPCA(n_components=1)
    >>> total_latency_for_pca = 100
    >>> latency_for_each = int(total_latency_for_pca / len(metrics))
    >>> X_dict = {}
    >>> for metric in metrics:
    ...     # load data. col header: Last GVT, shape of rest: (n, d). n: # of kps, d: # of time points
    ...     file_name = './data/ross-df-256kp-500gvt_' + metric + '.csv'
    ...     metric_nd = pd.read_csv(file_name)
    ...     pca.progressive_fit(
    ...         metric_nd,
    ...         latency_limit_in_msec=latency_for_each,
    ...         point_choice_method='reverse')
    ...     metric_1d = pca.transform(metric_nd)
    ...     X_dict[metric] = metric_1d.flatten().tolist()
    >>> X = pd.DataFrame(X_dict)

    >>> # causality analysis
    >>> causality = Causality()
    >>> causality.adaptive_progresive_var_fit(X, latency_limit_in_msec=100)
    >>> causality_from, causality_to = causality.check_causality('RbSec', signif=0.1)
    >>> ir_from, ir_to = causality.impulse_response('RbSec')
    >>> vd_from, vd_to = causality.variance_decomp('RbSec')

    >>> # print results
    >>> print('All KPs')
    >>> print('Rbsec (caused by)')
    >>> print(
    ...     pd.DataFrame({
    ...         'Metrics': metrics,
    ...         'Causality': causality_from,
    ...         'IR 1 step later': ir_from[:, 1],
    ...         'VD 1 step later': vd_from[:, 1]
    ...     }))

    >>> print('Rbsec (causing to)')
    >>> print(
    ...     pd.DataFrame({
    ...         'Metrics': metrics,
    ...         'Causality': causality_to,
    ...         'IR 1 step later': ir_to[:, 1],
    ...         'VD 1 step later': vd_to[:, 1]
    ...     }))
    Notes
    -----
    VAR: (TODO put references)
    Order selection for VAR: (TODO put references)
    Granger Causality: (TODO put references)
    Impulse Response Function: (TODO put references)
    Variance decompositon: (TODO put references)
    References
    ----------
    """

    def __init__(self):
        self.var_result = None
        self.n_processed_in_prev_var_fit = 0
        self.duration_in_prev_var_fit = 0

    def var_fit(self, endog, maxlags=5, ic='aic', verbose=False, trend='c'):
        '''
        Find best VAR with best order and various lags
        Parameters
        ----------
        endog : array-like, (shape: (n_time_points, n_variables))
            2-d endogenous response variable. The independent variable.
        maxlags : int
            Maximum number of lags to check for order selection.
        ic : {'aic', 'fpe', 'hqic', 'bic', None}, optional, (default="aic")
            Information criterion to use for VAR order selection.
            aic : Akaike
            fpe : Final prediction error
            hqic : Hannan-Quinn
            bic : Bayesian a.k.a. Schwarz
        verbose : bool, default False
            Print order selection output to the screen
        trend : str {"c", "ct", "ctt", "nc"}, optional, (default="c")
            "c" - add constant
            "ct" - constant and trend
            "ctt" - constant, linear and quadratic trend
            "nc" - co constant, no trend
            Note that these are prepended to the columns of the dataset.
        Notes
        -----
        Returns
        -------
        self (updating self.var_result)
        '''
        self.var_result = VAR(endog).fit(
            maxlags=maxlags, ic=ic, verbose=verbose, trend=trend)
        print('a', self.var_result.neqs)

    def adaptive_progresive_var_fit(self,
                                    endog,
                                    latency_limit_in_msec=1000,
                                    point_choice_method='random',
                                    maxlags=5,
                                    ic='aic',
                                    verbose=False,
                                    trend='c'):
        '''
        Find best VAR with best order and various lags with a progressive
        manner by adaptively changing the number of time points used for VAR.
        Parameters
        ----------
        endog : array-like (shape: (n_time_points, n_variables))
            2-d endogenous response variable. The independent variable.
        latency_limit_in_msec: int, optional, (default=1000)
            Latency limit for var_fit. Once total duration time passed this
            time, the var_fit will be stopped.
        point_choice_method: string, optional, (default="random")
            Point selection method from all n_time_points. Options are as below.
            "random": randomly select time points for each loop.
            "as_is": select time points in the order of time points as it is
                in endog for each loop.
            "reverse": select time points in the reverse order of time points
                in endog for each loop.
        maxlags : int, optional, (default=5)
            Maximum number of lags to check for order selection.
        ic : {'aic', 'fpe', 'hqic', 'bic', None}, optional, (default="aic")
            Information criterion to use for VAR order selection.
            aic : Akaike
            fpe : Final prediction error
            hqic : Hannan-Quinn
            bic : Bayesian a.k.a. Schwarz
        verbose : bool, optional, (default=False)
            Print order selection output and how many data points are processsed
            during adaptive_progresive_var_fit to the screen.
        trend : str {"c", "ct", "ctt", "nc"}, optional, (default="c")
            "c" - add constant
            "ct" - constant and trend
            "ctt" - constant, linear and quadratic trend
            "nc" - co constant, no trend
            Note that these are prepended to the columns of the dataset.
        Notes
        -----
        Returns
        -------
        self (updating self.var_result)
        '''
        start_time = time.time()
        n, _ = endog.shape
        latency_limit = latency_limit_in_msec / 1000.0

        order = [i for i in range(n)]
        if point_choice_method == 'random':
            shuffle(order)
        elif point_choice_method == 'as_is':
            None  # Do nothing
        elif point_choice_method == 'reverse':
            order.reverse()
        else:
            print("point_choice_method-", point_choice_method,
                  " is not supported. We used as_is instead of this.")
        duration = 0
        while True:
            loop_start_time = time.time()
            # because var_fit's time complexity is O(dn^2),
            # we can estimate how many points we can handle

            # adaptively decide a number of points used for calculation
            if self.n_processed_in_prev_var_fit == 0:
                self.n_processed_in_prev_var_fit = 10
            else:
                if self.duration_in_prev_var_fit == 0:
                    self.n_processed_in_prev_var_fit += 10
                else:
                    remaining_time = latency_limit - duration
                    coeff = remaining_time / self.duration_in_prev_var_fit
                    if coeff > 1.0:
                        self.n_processed_in_prev_var_fit = math.floor(
                            self.n_processed_in_prev_var_fit *
                            math.sqrt(coeff))

            if self.n_processed_in_prev_var_fit > n:
                self.n_processed_in_prev_var_fit = n

            # because of a bug in order selection of VAR model fit when number of time
            # points are small, we need to handle exception
            try:
                self.var_fit(
                    endog.iloc[order[:self.n_processed_in_prev_var_fit]],
                    maxlags=maxlags,
                    ic=ic)
            except:
                print("order selection doesn't work well")
                self.var_fit(
                    endog.iloc[order[:self.n_processed_in_prev_var_fit]],
                    maxlags=maxlags,
                    ic=None,
                    verbose=verbose,
                    trend=trend)

            self.duration_in_prev_var_fit = time.time() - loop_start_time

            duration = time.time() - start_time

            if (duration >= latency_limit
                    or self.n_processed_in_prev_var_fit >= n):
                break

        # if completion time is slower than latencyLimitInMSec,
        # update n_processed_in_prev_var_fit with smaller value
        if self.duration_in_prev_var_fit > latency_limit:
            coeff = latency_limit / self.duration_in_prev_var_fit
            self.n_processed_in_prev_var_fit = math.floor(
                self.n_processed_in_prev_var_fit * math.sqrt(coeff))

        if verbose:
            print("adaptive_progresive_var_fit(): ",
                  self.n_processed_in_prev_var_fit, " of ", n,
                  "data points processed in ", duration * 1000.0, " msec.")

    def check_causality(self, target, kind='f', signif=0.05):
        """
        Test Granger causality to and from the indicated target.
        Parameters
        ----------
        target: int or str
            Column index or name indicating the target in the endog used for
            var_fit or adaptive_progresive_var_fit, which will be checked
            causality.
        kind : {'f', 'wald'}, optional, (default='f')
            Perform F-test or Wald (chi-sq) test
        signif : float, , optional, (default=0.05 (i.e., 5%))
            Significance level for computing critical values for test,
            defaulting to standard 0.05 level
        Returns
        ----------
        caused_by: boolean list
            Causality test results of target <- others. The order is
            corresponding to the column indices of the endog.
        caused_to: boolean list
            Causality test results of target -> others. The order is
            corresponding to the column indices of the endog.
        """
        caused_by = None
        causing_to = None

        if self.var_result == None:
            print("Need to apply var_fit before check_causality")
        else:
            d = self.var_result.neqs
            print(d)
            caused_by = [
                self.var_result.test_causality(
                    caused=target, causing=i, kind=kind).pvalue < signif
                for i in range(0, d)
            ]
            causing_to = [
                self.var_result.test_causality(
                    caused=i, causing=target, kind=kind).pvalue < signif
                for i in range(0, 7)
            ]
            # replace target->target results as None
            target_idx = target
            if type(target) == str:
                target_idx = self.var_result.names.index(target)
            caused_by[target_idx] = None
            causing_to[target_idx] = None

        return caused_by, causing_to

    def impulse_response(self, target, periods=1):
        """
        Analyze impulse responses to shocks in system.
        Parameters
        ----------
        periods : int, optional (default=1)
            The range of time periods which will be analyzed impulse response.
            For example, if periods=1, the method will check the impulse
            response for the current and next time steps.
        Returns
        ----------
        ir_caused_by: array-like, shape(n_variables, periods+1)
            Impulse responses of target <- others. The order is
            corresponding to the column indices of the endog.
        ir_causing_to: array-like, shape(n_variables, periods+1)
            Impulse responses of target -> others. The order is
            corresponding to the column indices of the endog.
        """
        ir_caused_by = None
        ir_causing_to = None

        if self.var_result == None:
            print("Need to apply var_fit before impulse_response")
        else:
            irf = self.var_result.irf(periods=periods).orth_irfs

            target_idx = target
            if type(target) == str:
                target_idx = self.var_result.names.index(target)
            d = self.var_result.neqs

            ir_caused_by = [irf[:, target_idx, i] for i in range(d)]
            ir_causing_to = [irf[:, i, target_idx] for i in range(d)]

        return np.array(ir_caused_by), np.array(ir_causing_to)

    def variance_decomp(self, target, periods=1):
        """
        Compute forecast error variance decomposition ("FEVD")
        Parameters
        ----------
        periods : int, optional (default=1)
            The range of time periods which will be computed FEVD.
            For example, if periods=1, the method will check the impulse
            response for the current and next time steps.
        Returns
        ----------
        vd_caused_by: array-like, shape(n_variables, periods+1)
            FEVD of target <- others. The order is corresponding to the column
            indices of the endog.
        vd_causing_to: array-like, shape(n_variables, periods+1)
            FEVD of target -> others. The order is corresponding to the column
            indices of the endog.
        """
        vd_caused_by = None
        vd_causing_to = None

        if self.var_result == None:
            print("Need to apply var_fit before variance_decomp")
        else:
            vd = self.var_result.fevd(periods=periods + 1).decomp

            target_idx = target
            if type(target) == str:
                target_idx = self.var_result.names.index(target)
            d = self.var_result.neqs

            vd_caused_by = [vd[target_idx, :, i] for i in range(d)]
            vd_causing_to = [vd[i, :, target_idx] for i in range(d)]

        return np.array(vd_caused_by), np.array(vd_causing_to)

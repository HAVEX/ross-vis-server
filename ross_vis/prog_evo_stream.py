#  Because pybind11 cannot generate default parameters well, this code is to set them

from clustering.prog_evo_stream import prog_evo_stream_cpp


class ProgEvoStream(prog_evo_stream_cpp.ProgEvoStream):
    """Progressive usage of evoStream (stream clusterin method).
    Modified implementation of evoStream [Carnein and Trautmann, 2018] and
    implementation of the progressive usage of evoStream.
    Parameters
    ----------
    n_clusters: int, optional, (default=3)
        The number of clusters to form as well as the number of centroids to
        generate.
    r: float, (default=0.02)
        Radius threshold for micro-cluster assignment.
    tgap: int, optional, (default=10)
        Time-interval between outlier detection and clean-up
    lambd: float, optional, (default=0.0)
        Decay rate.
    crossover_rate: float, optional, (default=0.8)
        Cross-over rate for the evolutionary algorithm.
    mutation_rate: float, optional, (default=0.001)
        Mutation rate for the evolutionary algorithm.
    population_size: int, optional, (default=100)
        Number of solutions that the evolutionary algorithm maintains.
    initialize_after: int, optional, (default=0)
        Number of micro-cluster required for the initialization of the
        evolutionary algorithm.
    refine_cluster_generations: int, optional, (default=250)
        Number of EA generations performed during refining clusters
    Attributes
    ----------
    Examples
    --------
    >>> from prog_evo_stream import ProgEvoStream
    >>> import numpy as np
    >>> X = np.array([[1, 2], [1, 4], [1, 0], [4, 2], [4, 4], [4, 0]])
    >>> evo = ProgEvoStream(n_clusters=3)
    >>> evo.progressive_fit(X, latency_limit_in_msec=10)
    >>> labels = evo.predict(X)
    >>> labels
    [0, 0, 1, 2, 2, 2]
    >>> # add one new feature for each data point
    >>> X = np.array([[1, 2, 4], [1, 4, 4], [1, 0, 4],
    ...               [4, 2, 4], [4, 4, 4], [4, 0, 4]])
    >>> evo.progressive_fit(X, latency_limit_in_msec=10)
    >>> evo.predict(X)
    [1, 1, 1, 0, 2, 0]
    >>> # convert current labels to consistent labels with previous labels
    >>> labels, _ = ProgEvoStream.consistent_labels(labels, evo.predict(X))
    >>> labels
    [0, 0, 0, 2, 0, 2]
    Notes
    -----
    evoStream is from:
    `M. Carnein, H. Trautmann, evoStream – Evolutionary Stream Clustering
    Utilizing Idle Times, Big Data Research, Volume 14,
    pp. 101-111, 2018.`
    The code provided in
    https://wiwi-gitlab.uni-muenster.de/m_carn01/evostream_c
    are modified to improve performance, make consistent interfaces following
    scikit-learn ways, and add some useful features.
    Also, progressive usage of evoStream (returning the best result within the
    stated latency) is added here.
    References
    ----------
     M. Carnein, H. Trautmann, evoStream – Evolutionary Stream Clustering
      Utilizing Idle Times, Big Data Research, Volume 14,
      pp. 101-111, 2018.
    """

    def __init__(self,
                 n_clusters=3,
                 r=0.02,
                 tgap=10,
                 lambd=0.0,
                 crossover_rate=0.8,
                 mutation_rate=0.001,
                 population_size=100,
                 initialize_after=0,
                 refine_cluster_generations=250):
        super().__init__(n_clusters, r, tgap, lambd, crossover_rate,
                         mutation_rate, population_size, initialize_after,
                         refine_cluster_generations)

    def progressive_fit(self,
                        X,
                        latency_limit_in_msec=1000,
                        point_choice_method="fromPrevMicro",
                        verbose=False):
        """Progressive fit with data points, X. With this, clusters (or macro
        clusters) and micro clusters are updated progressively and
        incrementally within an indicated latency limit.
        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            Training data, where n_samples is the number of samples and
            n_features is the number of features.
        latency_limit_in_msec: int, optional, (default=1000)
            Latency limit for incremental fits. Once total duration time passed
            this time, the incremental update will be stopped.
        point_choice_method: string, optional, (default="fromPrevMicro")
            Point selection method from all n_samples. Options are as below.
            "random": randomly select one data point for each incremental
                update.
            "from_prev_macro": use labels to macro clusters of previous
                progressive fit result. For each incremental update, select one
                data point from each edifferent macro cluster.
            "from_prev_micro": use labels to micro clusters of previous
                progressive fit result. For each incremental update, select one
                data point from each edifferent micro cluster.
            "as_is": select one data point in the order of data points as it is
                in X for each incremental update.
            "reverse": select one data point in the reverse order of data points
                in X for each incremental update.
        verbose: boolean, optional (default=False)
            If True, print out how many data points are processsed during
            progressive_fit.
        Returns
        -------
        self : object
            Returns the instance itself.
        """
        return super().progressive_fit(X, latency_limit_in_msec,
                                       point_choice_method, verbose)

    def partial_fit(self, X):
        """Incremental fit with new datapoints, X. With this, clusters (or macro
        clusters) and micro clusters are updated.

        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            Training data, where n_samples is the number of samples and
            n_features is the number of features.
        Returns
        -------
        self : object
            Returns the instance itself.
        """
        return super().partial_fit(X)

    def predict(self, X):
        """Predict the closest macro cluster each sample in X belongs to.
        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            Training data, where n_samples is the number of samples and
            n_features is the number of features.
        Returns
        -------
        labels : array, shape [n_samples,]
            Index of the macro cluster each sample belongs to.
        """
        return super().predict(X)

    def refine_cluster(self, generations=1):
        """Refine current cluster with evolution.
        Parameters
        ----------
        generations : int, optional, (default = 1)
            # of generation in evolution.
        Returns
        -------
        self : object
            Returns the instance itself.
        """
        return super().refine_cluster(generations)

    def progressive_refine_cluster(self,
                                   latency_limit_in_msec=1000,
                                   verbose=False):
        """Refine current cluster with evolution in latency_limit_in_msec.
        Parameters
        ----------
        latency_limit_in_msec: int, optional, (default=1000)
            Latency limit for incremental fits. Once total duration time passed
            this time, the incremental update will be stopped.
        verbose: boolean, optional (default=False)
            If True, print out how many data points are processsed during
            progressive process.
        Returns
        -------
        self : object
            Returns the instance itself.
        """
        return super().progressive_refine_cluster(latency_limit_in_msec,
                                                  verbose)

    @classmethod
    def consistent_labels(self,
                          prev_labels,
                          current_labels,
                          latency_limit_in_msec=1000,
                          verbose=False):
        """Find consistent labels with previous labels to avoid dramatically
        changing cluster labels from previous to current.
        Parameters
        ----------
        prev_labels : array, shape [n_samples, ]
            Cluster labels obtained in last progressiveFit.
        current_labels : array, shape [n_samples, ]
            Cluster labels obtained in current progressiveFit.
        Returns
        -------
        labels : array, shape [n_samples,]
            Index of the macro cluster each sample belongs to.
        current_label_to_previous_label : dictionary
            Dictionary from the current label to the corresponding previous
            label.
        verbose: boolean, optional (default=False)
            If True, print out how many data points are processsed during
            progressive process.
        """
        return super().consistent_labels(prev_labels, current_labels,
                                         latency_limit_in_msec, verbose)

    def get_macro_clusters(self):
        """Get macro cluster centers.
        Parameters
        ----------
        None
        Returns
        -------
        cluster_centers : array, [n_macro_clusters, n_features]
            Coordinates of macro cluster centers.
        """
        return super().get_macro_clusters()

    def get_micro_clusters(self):
        """Get micro cluster centers.
        Parameters
        ----------
        None
        Returns
        -------
        cluster_centers : array, [n_micro_clusters, n_features]
            Coordinates of micro cluster centers.
        """
        return super().get_micro_clusters()

    def micro_to_macro(self):
        """Get each micro cluster's corresponding macro cluster number.
        Parameters
        ----------
        None
        Returns
        -------
        labels : array, [n_micro_clusters,]
            Index of the macro cluster each micro cluster belongs to.
        """
        return super().micro_to_macro()

    def point_to_macro(self, X):
        """The same function with predict(X).
        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            Training data, where n_samples is the number of samples and
            n_features is the number of features.
        Returns
        -------
        labels : array, shape [n_samples,]
            Index of the macro cluster each sample belongs to.
        """
        return super().point_to_macro(X)

    def point_to_micro(self, X):
        """Predict the closest micro cluster each sample in X belongs to.
        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            Training data, where n_samples is the number of samples and
            n_features is the number of features.
        Returns
        -------
        labels : array, shape [n_samples,]
            Index of the micro cluster each sample belongs to.
        """
        return super().point_to_micro(X)

    def reset(self):
        """Reset all the information related with clusters
        Parameters
        ----------
        None.
        Returns
        -------
        self : object
            Returns the instance itself.
        """
        return super().reset(X)

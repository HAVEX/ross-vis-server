#  Because pybind11 cannot generate default parameters well, this code is to set them

from dim_reduction.prog_inc_pca import prog_inc_pca_cpp


class ProgIncPCA(prog_inc_pca_cpp.ProgIncPCA):
    """Incremental principal components analysis.
    Implementation of the incremental PCA of Ross et al., 2008 and the
    geometric transformation, position estimation, uncertatinty measures by
    Fujiwara et al.

    Parameters
    ----------
    n_components : int, (default=2)
        Number of components to keep. If n_components is 0,
        then n_components is set to min(n_samples, n_features).
    forgetting_factor: float, (default=1.0)
        A forgetting factor, f,  provides a way to reduce the contributions of
        past observations to the latest result. The value of f ranges from 0 to
        1 where f = 1 means no past results will be forgotten. When 0 <= f < 1,
        the contributions of past observations gradually decrease as new data
        points are obtained. As described in [Ross et al., 2008], the effective
        size of the observation history (a number of observations which affect
        the PCA result) equals to m/(1 - f) (m: number of new data points). For
        example, when f = 0.98 and m = 2, the most recent 100 observations are
        effective.
    Attributes
    ----------
    Examples
    --------
    >>> import matplotlib.pyplot as plt
    >>> import numpy as np
    >>> from sklearn import datasets
    >>> from prog_inc_pca import ProgIncPCA

    >>> # load data
    >>> iris = datasets.load_iris()
    >>> X = iris.data
    >>> group = iris.target
    >>> target_names = iris.target_names

    >>> # apply PCA
    >>> pca = ProgIncPCA(2, 1.0)
    >>> pca.progressive_fit(X, latency_limit_in_msec=10)
    >>> Y_a = pca.transform(X)
    >>> pca.get_loadings()

    >>> # plot results
    >>> plt.figure()
    >>> colors = ['navy', 'turquoise', 'darkorange']
    >>> lw = 2
    >>> for color, i, target_name in zip(colors, [0, 1, 2], target_names):
    ...     plt.scatter(
    ...         Y_a[group[0:len(Y_a)] == i, 0],
    ...         Y_a[group[0:len(Y_a)] == i, 1],
    ...         color=color,
    ...         alpha=.8,
    ...         lw=lw,
    ...         label=target_name)
    >>> plt.legend(loc='best', shadow=False, scatterpoints=1)
    >>> plt.title('Progressive PCA result for Iris')

    >>> # add one new feature for each data point and apply PCA again
    >>> new_feature = np.random.rand(X.shape[0], 1) * X.max() * 0.5
    >>> X = np.append(X, new_feature, 1)
    >>> pca = ProgIncPCA(2, 1.0)
    >>> pca.progressive_fit(X, latency_limit_in_msec=10)
    >>> Y_b = pca.transform(X)

    >>> # plot results without geometric transformation
    >>> plt.figure()
    >>> for color, i, target_name in zip(colors, [0, 1, 2], target_names):
    ...     plt.scatter(
    ...         Y_b[group[0:len(Y_b)] == i, 0],
    ...         Y_b[group[0:len(Y_b)] == i, 1],
    ...         color=color,
    ...         alpha=.8,
    ...         lw=lw,
    ...         label=target_name)
    >>> plt.legend(loc='best', shadow=False, scatterpoints=1)
    >>> plt.title('After adding new feature (without geom_trans)')

    >>> # apply the progressive geometric transformation
    >>> Y_bg = ProgIncPCA.geom_trans(Y_a, Y_b)

    >>> plt.figure()
    >>> for color, i, target_name in zip(colors, [0, 1, 2], target_names):
    ...     plt.scatter(
    ...         Y_bg[group[0:len(Y_bg)] == i, 0],
    ...         Y_bg[group[0:len(Y_bg)] == i, 1],
    ...         color=color,
    ...         alpha=.8,
    ...         label=target_name)
    >>> plt.legend(loc='best', shadow=False, scatterpoints=1)
    >>> plt.title('After adding new feature (with geom_trans)')
    >>> plt.show()
    Notes
    -----
    The incremental PCA model is from:
    `D. Ross, J. Lim, R. Lin, M. Yang, Incremental Learning for Robust Visual
    Tracking, International Journal of Computer Vision, Volume 77, Issue 1-3,
    pp. 125-141, 2008.`
    The geometric transformation, position estimation, and uncertatinty measures
    are from:
    `T. Fujiwara, J.-K. Chou, Shilpika, P. Xu, L. Ren, K.-L. Ma, Incremental
    Dimensionality Reduction Method for Visualizing Streaming Multidimensional
    Data.`
    The version of implementation in Scikit-learn was refered to implement the
    incremental PCA of Ross et al, 2008. However, this implementation includes
    various modifications (simplifying the parameters, adding forgetting factor,
    etc).
    Incremental PCA in Scikit-learn:
    http://scikit-learn.org/stable/auto_examples/decomposition/plot_incremental_pca.html
    References
    ----------
     D. Ross, J. Lim, R. Lin, M. Yang. Incremental Learning for Robust Visual
        Tracking, International Journal of Computer Vision, Volume 77,
        Issue 1-3, pp. 125-141, 2008.
     T. Fujiwara, J.-K. Chou, Shilpika, P. Xu, L. Ren, K.-L. Ma, Incremental
        Dimensionality Reduction Method for Visualizing Streaming
        Multidimensional Data.
    """

    def __init__(self, n_components=2, forgetting_factor=1.0):
        super().__init__(n_components, forgetting_factor)

    def initialize(self):
        return super().initialize()

    def progressive_fit(self,
                        X,
                        latency_limit_in_msec=1000,
                        point_choice_method="random",
                        verbose=False):
        """Progressive fit with data points, X. With this, cPCs are updated
        from previous results progressively and incrementally within an
        indicated latency limit. X's row (i.e., number of data points) must be
        greater than or equal to 2.
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
        super().progressive_fit(X, latency_limit_in_msec, point_choice_method,
                                verbose)

    def partial_fit(self, X):
        """Incremental fit with new datapoints, X. With this, PCs are updated
        from previous results incrementally. X's row (i.e., number of data
        points) must be greater than or equal to 2.

        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            Training data, where n_samples is the number of samples and
            n_features is the number of features. n_samples must be >= 2.
            n_features and n_samples must be >= n_components. Also, n_features
            must be the same size with the first X input to partial_fit.
        Returns
        -------
        self : object
            Returns the instance itself.
        """
        return super().partial_fit(X)

    def transform(self, X):
        """Obtaining transformed result Y with X and current PCs.

        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            Testing data, where n_samples is the number of samples and
            n_features is the number of features. n_features must be the same
            size with the traiding data's features used for partial_fit.
        Returns
        -------
        Y : array-like, shape (n_samples, n_components)
            Returns the transformed (or projected) result.
        """
        return super().transform(X)

    def get_loadings(self):
        """Obtaining current PC loadings.

        Returns
        -------
        W : array-like, shape (n_components, n_features)
            Returns PC loadings.
        """
        return super().get_loadings()

    @classmethod
    def geom_trans(cls, Y1, Y2):
        """Finding the geometric transformation matrix, which maximizes the
        Y2's overlap to Y1  with a combination of uniform scaling, rotation,
        and sign flipping.

        Parameters
        ----------
        Y1 : array-like, shape (n_samples, n_dimensions)
            Data point positions (n_dimensions). Y1 is used as a base of ovarlapping.
        Y2 : array-like, shape (n_samples, n_dimensions)
            Data point positions (n_dimensions). geom_trans finds a matrix to optimally
            overlap Y2 to Y1.
        Returns
        -------
        Y2_g: array-like, shape (n_samples, n_dimensions)
            Y2 after applied the geometric transformation.
        """
        return super().geom_trans(Y1, Y2)

    def get_uncert_v(self, n_obtained_features):
        """Obtaining the uncertainty measure, V, introduced in
        [Fujiwara et al., xxxx] with current PCA result and a number of
        obtained features of a new data point.

        Parameters
        ----------
        n_obtained_features : int
            Number of obtained features of a new data point.
            n_obtained_features must be <= n_components.
        Returns
        -------
        V : double
            Returns the uncertainty measure, V. V will be 0, 1 if
            n_obtained_features = 0, n_components, respectively.
        """
        return super().get_uncert_v(n_obtained_features)

    @classmethod
    def pos_est(cls, p, Y1, Y2):
        """Estaimated position of p. Refer [Fujiwara et al., xxxx].

        Parameters
        ----------
        p: array-like, shape(1, 2)
            A data point position trasformed with PCA of d=l (l<=D).
        Y1 : array-like, shape (n_samples, 2)
            Data point positions (2D). Y1 shoule be a transformed position with
            PCA of d=l (l<=D).
        Y2 : array-like, shape (n_samples, 2)
            Data point positions (2D). Y2 shoule be a transformed position with
            PCA of d=D.
        Returns
        -------
        est_pos : array-like, shape(1, 2)
            Returns the estimated 2D position.
        uncert_u: float
            Reutrn the uncertainty measure, U
        """
        estPosAndUncertU = super().pos_est(p, Y1, Y2)
        return estPosAndUncertU[0], estPosAndUncertU[1]

    @classmethod
    def update_uncert_weight(cls, current_gamma, current_sq_grad,
                             current_sq_dgamma, sigma, sprimes, uncert_us,
                             uncert_vs):
        """Update the combined uncertainty weight, gamma.
        Refer [Fujiwara et al., xxxx].

        Parameters
        ----------
        current_gamma: float
            Current gamma obtained Eq. 12 in [Fujiwara, xxx].
        current_sq_grad: float
            Used in Adadelta calculation in Eq. 11. Set 0 as an initial value.
        current_sq_dgamma: float
            Used in Adadelta calculation in Eq. 11. Set 0 as an initial value.
        sigma: array-like, shape(m, n)
            Distance between m new data positions and n exisiting data positions
             in the PCA result after new data points obtain D dimensions.
        sprimes: array-like, D x shape(m, n)
            Distances between m esitmated data positions and n exisiting data
            positions for each dimension l (1 <= l <= D).
        uncert_us: array-like, shape(D, m)
            Uncertainty measure U for m new data points for each dimension l.
        uncert_vs: array-like, shape(D)
            Uncertainty measure V for each dimension l.
        Returns
        -------
        updated_gamma: float
        updated_sq_grad: float
        updated_sq_dgamma: float
            Use these updated values for the next run of update_uncert_weight
        """
        result = super().update_uncert_weight(current_gamma, current_sq_grad,
                                              current_sq_dgamma, sigma,
                                              sprimes, uncert_us, uncert_vs)
        return result[0], result[1], result[2]

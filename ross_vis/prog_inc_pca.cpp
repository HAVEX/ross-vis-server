#include "prog_inc_pca.hpp"

#include <chrono>
#include <iostream>

/* References
----------
T. Fujiwara, J.-K. Chou, Shilpika, P. Xu, L. Ren, K.-L. Ma. An Incremental
Dimensionality Reduction Method for Visualizing Streaming Multidimensional
Data
*/

ProgIncPCA::ProgIncPCA(Eigen::Index const nComponents,
                       double const forgettingFactor)
    : IncPCA(nComponents, forgettingFactor) {}

void ProgIncPCA::progressiveFit(Eigen::MatrixXd const &X,
                                unsigned int const latencyLimitInMSec,
                                std::string const pointChoiceMethod,
                                bool const verbose) {
  auto startTime = std::chrono::steady_clock::now();

  auto n = X.rows();
  auto d = X.cols();

  if (n < 2) {
    std::cerr << "# of data points must be at least 2" << std::endl;
  } else {

    std::vector<unsigned int> order;
    // get order of indices which will be processed
    if (pointChoiceMethod == "random") {
      order = randomOrder(n);
    } else if (pointChoiceMethod == "asIs" || pointChoiceMethod == "as_is") {
      order.reserve(n);
      for (unsigned int i = 0; i < n; ++i) {
        order.push_back(i);
      }
    } else if (pointChoiceMethod == "reverse") {
      order.reserve(n);
      for (unsigned int i = n; i > 0; --i) {
        order.push_back(i - 1);
      }
    } else {
      std::cout << "there is no point choice method called "
                << pointChoiceMethod << ". So, \"random\" is applied."
                << std::endl;
      order = randomOrder(n);
    }

    auto duration = 0;
    unsigned int nProcessed = 0;
    do {
      auto nRemains = n - nProcessed;
      if (nRemains >= 2 && nRemains != 3) {
        partialFit(X.block(nProcessed, 0, 2, d));
        nProcessed += 2;
      } else if (nRemains == 3) {
        partialFit(X.block(nProcessed, 0, 3, d));
        nProcessed += 3;
      }

      duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - startTime)
                     .count();
    } while (duration < latencyLimitInMSec && nProcessed < n);

    if (verbose) {
      std::cout << "progressiveFit(): " << nProcessed << " of " << n
                << " data points processed in " << duration << " msec."
                << std::endl;
    }
  }
}

std::vector<unsigned int> ProgIncPCA::randomOrder(unsigned int const n) {
  std::vector<unsigned int> order;
  order.reserve(n);
  for (unsigned int i = 0; i < n; ++i) {
    order.push_back(i);
  }
  std::random_shuffle(order.begin(), order.end());

  return order;
}

void ProgIncPCA::shuffleMatrix2D(Eigen::MatrixX2d &X1, Eigen::MatrixX2d &X2) {
  auto n1 = X1.rows();
  auto n2 = X2.rows();
  auto n = std::min(n1, n2);
  Eigen::VectorXi indices = Eigen::VectorXi::LinSpaced(n, 0, n);
  std::random_shuffle(indices.data(), indices.data() + n);

  if (n1 == n) {
    X1 = indices.asPermutation() * X1;
  } else {
    X1.topRows(n) = indices.asPermutation() * X1.topRows(n);
  }

  if (n2 == n) {
    X2 = indices.asPermutation() * X2;
  } else {
    X2.topRows(n) = indices.asPermutation() * X2.topRows(n);
  }
}

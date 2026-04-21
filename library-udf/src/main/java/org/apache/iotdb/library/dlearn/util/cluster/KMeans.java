/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.library.dlearn.util.cluster;

import java.util.Arrays;

/**
 * Univariate subsequence k-means (Lloyd); optionally z-normalize, then cluster in Euclidean space.
 */
public class KMeans {

  private double[][] centroids;
  private int[] labels;

  public void fit(double[][] samples, int k, boolean normalize, int maxIterations) {
    validate(samples, k, maxIterations);
    int n = samples.length;
    int dim = samples[0].length;

    double[][] z = new double[n][dim];
    for (int i = 0; i < n; i++) {
      z[i] = ClusterUtils.maybeZNormalize(samples[i], normalize);
    }

    centroids = new double[k][dim];
    for (int c = 0; c < k; c++) {
      System.arraycopy(z[c], 0, centroids[c], 0, dim);
    }

    labels = new int[n];
    Arrays.fill(labels, -1);

    for (int iter = 0; iter < maxIterations; iter++) {
      double[][] prevCentroids = new double[k][dim];
      for (int c = 0; c < k; c++) {
        System.arraycopy(centroids[c], 0, prevCentroids[c], 0, dim);
      }

      boolean changed = false;
      for (int i = 0; i < n; i++) {
        int best = 0;
        double bestDist = Double.POSITIVE_INFINITY;
        for (int c = 0; c < k; c++) {
          double d = ClusterUtils.squaredEuclidean(z[i], centroids[c]);
          if (d < bestDist) {
            bestDist = d;
            best = c;
          }
        }
        if (labels[i] != best) {
          labels[i] = best;
          changed = true;
        }
      }

      double[][] newCentroids = new double[k][dim];
      int[] counts = new int[k];
      for (int i = 0; i < n; i++) {
        int c = labels[i];
        counts[c]++;
        for (int d = 0; d < dim; d++) {
          newCentroids[c][d] += z[i][d];
        }
      }
      for (int c = 0; c < k; c++) {
        if (counts[c] == 0) {
          int donor = ClusterUtils.findLargestCluster(counts);
          System.arraycopy(prevCentroids[donor], 0, centroids[c], 0, dim);
          for (int d = 0; d < dim; d++) {
            centroids[c][d] += (d == 0 ? 1e-4 : -1e-4);
          }
        } else {
          for (int d = 0; d < dim; d++) {
            centroids[c][d] = newCentroids[c][d] / counts[c];
          }
        }
      }

      if (!changed) {
        break;
      }
    }
  }

  public double[][] getCentroids() {
    return centroids;
  }

  public int[] getLabels() {
    return labels;
  }

  private static void validate(double[][] samples, int k, int maxIterations) {
    if (samples == null || samples.length == 0) {
      throw new IllegalArgumentException("samples must be non-empty.");
    }
    if (k < 2 || k > samples.length) {
      throw new IllegalArgumentException("k must satisfy 2 <= k <= samples.length.");
    }
    if (maxIterations < 1) {
      throw new IllegalArgumentException("maxIterations must be at least 1.");
    }
    int dim = samples[0].length;
    if (dim == 0) {
      throw new IllegalArgumentException("sample dimension must be positive.");
    }
    for (double[] row : samples) {
      if (row == null || row.length != dim) {
        throw new IllegalArgumentException("All samples must have the same length.");
      }
    }
  }
}

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

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.SingularValueDecomposition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * k-Shape: assignment uses {@link ClusterUtils#shapeDistance} (SBD = 1 − max NCC); centroids are
 * the first right singular vector of the cluster matrix from SVD, sign correction, then z-normalize
 * or L2 normalization.
 */
public class KShape {

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
          double d = ClusterUtils.shapeDistance(z[i], centroids[c]);
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

      int[] counts = new int[k];
      @SuppressWarnings("unchecked")
      List<double[]>[] byCluster = new List[k];
      for (int c = 0; c < k; c++) {
        byCluster[c] = new ArrayList<>();
      }
      for (int i = 0; i < n; i++) {
        int c = labels[i];
        counts[c]++;
        byCluster[c].add(z[i]);
      }

      for (int c = 0; c < k; c++) {
        if (counts[c] == 0) {
          int donor = ClusterUtils.findLargestCluster(counts);
          System.arraycopy(prevCentroids[donor], 0, centroids[c], 0, dim);
        } else {
          List<double[]> members = byCluster[c];
          double[][] mat = new double[members.size()][dim];
          for (int i = 0; i < members.size(); i++) {
            mat[i] = members.get(i);
          }
          centroids[c] = centroidFromSvd(mat, normalize);
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

  private static double[] centroidFromSvd(double[][] members, boolean zNormalizeCentroid) {
    int m = members.length;
    int dim = members[0].length;
    if (m == 1) {
      double[] u = Arrays.copyOf(members[0], dim);
      return zNormalizeCentroid ? ClusterUtils.zNormalize(u) : l2Unit(u);
    }
    RealMatrix y = MatrixUtils.createRealMatrix(members);
    SingularValueDecomposition svd = new SingularValueDecomposition(y);
    RealMatrix v = svd.getV();
    RealVector col0 = v.getColumnVector(0);
    double[] r = col0.toArray();
    double sumDot = 0.0;
    for (double[] row : members) {
      sumDot += dot(row, r);
    }
    if (sumDot < 0) {
      for (int i = 0; i < r.length; i++) {
        r[i] = -r[i];
      }
    }
    return zNormalizeCentroid ? ClusterUtils.zNormalize(r) : l2Unit(r);
  }

  private static double dot(double[] a, double[] b) {
    double s = 0.0;
    for (int i = 0; i < a.length; i++) {
      s += a[i] * b[i];
    }
    return s;
  }

  private static double[] l2Unit(double[] v) {
    double s = 0.0;
    for (double x : v) {
      s += x * x;
    }
    s = Math.sqrt(s);
    if (s < ClusterUtils.EPS) {
      return new double[v.length];
    }
    double[] o = new double[v.length];
    for (int i = 0; i < v.length; i++) {
      o[i] = v[i] / s;
    }
    return o;
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

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/**
 * Coarse clustering: {@link KMeans} uses {@code min(2k, n)} clusters (n = number of windows);
 * greedy fastKShape picks k representatives; both labels and the objective use {@link
 * ClusterUtils#maxNcc}.
 */
public class MedoidShape {

  private double sampleRate = 0.3;
  private Random random = new Random();

  /** Overrides the RNG used for greedy sampling (default is {@link Random#Random()}). */
  public void setRandom(Random random) {
    this.random = Objects.requireNonNull(random);
  }

  private double[][] centroids;
  private int[] labels;

  public void setSampleRate(double sampleRate) {
    if (sampleRate <= 0 || sampleRate > 1.0) {
      throw new IllegalArgumentException("sampleRate must be in (0, 1].");
    }
    this.sampleRate = sampleRate;
  }

  public double getSampleRate() {
    return sampleRate;
  }

  public void fit(double[][] samples, int k, boolean normalize, int maxIterations) {
    validate(samples, k, maxIterations);
    int n = samples.length;
    int dim = samples[0].length;

    int coarseK = Math.min(2 * k, n);

    double[][] x = new double[n][dim];
    for (int i = 0; i < n; i++) {
      x[i] = ClusterUtils.maybeZNormalize(samples[i], normalize);
    }

    KMeans coarse = new KMeans();
    coarse.fit(x, coarseK, false, maxIterations);
    double[][] euclideanCentroids = coarse.getCentroids();
    int[] kmLabels = coarse.getLabels();
    long[] clusterSize = new long[coarseK];
    for (int lb : kmLabels) {
      clusterSize[lb]++;
    }

    centroids = fastKShape(x, k, sampleRate, dim, euclideanCentroids, clusterSize, random);

    labels = new int[n];
    for (int i = 0; i < n; i++) {
      double maxNcc = Double.NEGATIVE_INFINITY;
      int label = -1;
      for (int j = 0; j < k; j++) {
        double cur = ClusterUtils.maxNcc(x[i], centroids[j]);
        if (cur > maxNcc) {
          maxNcc = cur;
          label = j;
        }
      }
      labels[i] = label;
    }
  }

  public double[][] getCentroids() {
    return centroids;
  }

  public int[] getLabels() {
    return labels;
  }

  private static double[][] fastKShape(
      double[][] x,
      int k,
      double r,
      int dim,
      double[][] euclideanCentroids,
      long[] clusterSize,
      Random rnd) {
    int n = x.length;
    if (n <= k) {
      double[][] out = new double[k][dim];
      for (int i = 0; i < n; i++) {
        out[i] = Arrays.copyOf(x[i], dim);
      }
      for (int i = n; i < k; i++) {
        out[i] = Arrays.copyOf(x[n - 1], dim);
      }
      return out;
    }

    List<double[]> picked = new ArrayList<>();
    Set<Integer> coresetIdx = new HashSet<>();

    for (int round = 0; round < k; round++) {
      List<Integer> pool = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        if (!coresetIdx.contains(i)) {
          pool.add(i);
        }
      }
      if (pool.isEmpty()) {
        throw new IllegalStateException("fastKShape: empty candidate pool.");
      }
      int sampleCount = Math.max(1, (int) (r * n));
      sampleCount = Math.min(sampleCount, pool.size());
      Collections.shuffle(pool, rnd);
      List<Integer> sampleIdx = pool.subList(0, sampleCount);

      double maxDelta = Double.NEGATIVE_INFINITY;
      double[] bestSeg = null;
      int bestIdx = -1;

      for (int idx : sampleIdx) {
        double[] seq = x[idx];
        picked.add(seq);
        double delta = evaluateAim(picked, euclideanCentroids, clusterSize);
        picked.remove(picked.size() - 1);
        if (delta > maxDelta) {
          maxDelta = delta;
          bestSeg = Arrays.copyOf(seq, dim);
          bestIdx = idx;
        }
      }

      if (bestSeg == null) {
        throw new IllegalStateException("fastKShape: no candidate selected.");
      }
      picked.add(bestSeg);
      coresetIdx.add(bestIdx);
    }

    double[][] out = new double[k][dim];
    for (int i = 0; i < k; i++) {
      out[i] = picked.get(i);
    }
    return out;
  }

  private static double evaluateAim(
      List<double[]> curCentroids, double[][] euclideanCentroids, long[] clusterSize) {
    double res = 0.0;
    for (int i = 0; i < euclideanCentroids.length; i++) {
      double maxNcc = Double.NEGATIVE_INFINITY;
      for (double[] cur : curCentroids) {
        double n = ClusterUtils.maxNcc(cur, euclideanCentroids[i]);
        if (n > maxNcc) {
          maxNcc = n;
        }
      }
      res += maxNcc * clusterSize[i];
    }
    return res;
  }

  private static void validate(double[][] samples, int k, int maxIterations) {
    if (samples == null || samples.length == 0) {
      throw new IllegalArgumentException("samples must be non-empty.");
    }
    if (k < 2) {
      throw new IllegalArgumentException("k must be at least 2.");
    }
    if (k > samples.length) {
      throw new IllegalArgumentException("k must not exceed the number of samples.");
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

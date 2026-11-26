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

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;

import java.util.Arrays;

/**
 * Subsequence z-normalize, Euclidean distance, and FFT-based NCC / SBD (shared by KShape and
 * MedoidShape).
 */
public final class ClusterUtils {

  public static final double EPS = 1e-9;

  private static final FastFourierTransformer FFT =
      new FastFourierTransformer(DftNormalization.STANDARD);

  private ClusterUtils() {}

  public static double[] maybeZNormalize(double[] a, boolean normalize) {
    if (normalize) {
      return zNormalize(a);
    }
    return Arrays.copyOf(a, a.length);
  }

  public static double[] zNormalize(double[] a) {
    int n = a.length;
    double sum = 0.0;
    for (double v : a) {
      sum += v;
    }
    double mean = sum / n;
    double var = 0.0;
    for (double v : a) {
      double d = v - mean;
      var += d * d;
    }
    var /= n;
    double std = Math.sqrt(Math.max(var, 0.0));
    double[] z = new double[n];
    if (std < EPS) {
      return z;
    }
    for (int i = 0; i < n; i++) {
      z[i] = (a[i] - mean) / std;
    }
    return z;
  }

  public static double squaredEuclidean(double[] a, double[] b) {
    double s = 0.0;
    for (int i = 0; i < a.length; i++) {
      double d = a[i] - b[i];
      s += d * d;
    }
    return s;
  }

  public static int findLargestCluster(int[] counts) {
    int best = 0;
    for (int i = 1; i < counts.length; i++) {
      if (counts[i] > counts[best]) {
        best = i;
      }
    }
    return best;
  }

  /**
   * Maximum over the normalized cross-correlation sequence (FFT); used for SBD and MedoidShape
   * objective.
   */
  public static double maxNcc(double[] x, double[] y) {
    double[] cc = nccFft(x, y);
    double max = Double.NEGATIVE_INFINITY;
    for (double v : cc) {
      if (v > max) {
        max = v;
      }
    }
    return max;
  }

  /** SBD: 1 − max NCC (consistent with the NCC-based definition in k-Shape / FastKShape). */
  public static double shapeDistance(double[] x, double[] y) {
    return 1.0 - maxNcc(x, y);
  }

  public static double symmetricSbd(double[] a, double[] b) {
    return 0.5 * (shapeDistance(a, b) + shapeDistance(b, a));
  }

  private static double[] nccFft(double[] x, double[] y) {
    int xLen = x.length;
    double den = l2Norm(x) * l2Norm(y);
    if (den < 1e-9) {
      den = Double.POSITIVE_INFINITY;
    }
    int fftSize = 1 << (32 - Integer.numberOfLeadingZeros(2 * xLen - 1));

    Complex[] cx = new Complex[fftSize];
    Complex[] cy = new Complex[fftSize];
    for (int i = 0; i < fftSize; i++) {
      cx[i] = new Complex(i < xLen ? x[i] : 0.0, 0.0);
      cy[i] = new Complex(i < xLen ? y[i] : 0.0, 0.0);
    }
    Complex[] fx = FFT.transform(cx, TransformType.FORWARD);
    Complex[] fy = FFT.transform(cy, TransformType.FORWARD);
    Complex[] prod = new Complex[fftSize];
    for (int i = 0; i < fftSize; i++) {
      prod[i] = fx[i].multiply(fy[i].conjugate());
    }
    Complex[] ccFull = FFT.transform(prod, TransformType.INVERSE);

    double[] ccPacked = new double[2 * xLen - 1];
    int p = 0;
    for (int i = fftSize - (xLen - 1); i < fftSize; i++) {
      ccPacked[p++] = ccFull[i].getReal() / den;
    }
    for (int i = 0; i < xLen; i++) {
      ccPacked[p++] = ccFull[i].getReal() / den;
    }
    return ccPacked;
  }

  private static double l2Norm(double[] v) {
    double s = 0.0;
    for (double x : v) {
      s += x * x;
    }
    return Math.sqrt(s);
  }
}

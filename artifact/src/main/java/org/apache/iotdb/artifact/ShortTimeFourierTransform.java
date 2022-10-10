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
package org.apache.iotdb.artifact;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.jtransforms.fft.DoubleFFT_1D;

/** Short time Fourier transform (STFT) with quantization */
public class ShortTimeFourierTransform {

  /**
   * transform given time domain data to frequency domain with quantization
   *
   * @param timeDomain given time domain data
   * @param blockSize block size for STFT
   * @param beta quantization level
   * @return quantized frequency domain
   */
  public static DoubleArrayList transform(DoubleArrayList timeDomain, int blockSize, double beta) {
    DoubleArrayList frequencyDomain = new DoubleArrayList(timeDomain.size());
    for (int i = 0; i < timeDomain.size(); i += blockSize) {
      double[] x = transformBlock(timeDomain, i, Math.min(timeDomain.size(), i + blockSize), beta);
      frequencyDomain.addAll(x);
    }
    return frequencyDomain;
  }

  private static double[] transformBlock(
      DoubleArrayList timeDomain, int from, int to, double beta) {
    int n = to - from;
    DoubleFFT_1D fft = new DoubleFFT_1D(n);
    double[] a = new double[2 * n];
    for (int i = 0; i < n; i++) {
      a[2 * i] = timeDomain.get(i + from);
      a[2 * i + 1] = 0;
    }
    fft.complexForward(a);
    double[] frequencyDomain = new double[n];
    double eps = Math.pow(2, beta);
    for (int i = 0; i < n; i++) {
      double v = Math.sqrt(a[2 * i] * a[2 * i] + a[2 * i + 1] * a[2 * i + 1]);
      v /= n;
      frequencyDomain[i] = (int) Math.round(v / eps) * eps;
    }
    return frequencyDomain;
  }
}

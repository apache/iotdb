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

package org.apache.iotdb.commons.udf.builtin.relational.tvf.fft;

/** Computes an in-place 1D forward DFT for interleaved complex double data. */
public final class DoubleFFT_1D {

  private final int length;

  public DoubleFFT_1D(long length) {
    if (length < 1 || length > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("FFT length must be a positive int-sized value.");
    }
    this.length = (int) length;
  }

  public void complexForward(double[] values) {
    if (values.length < 2 * length) {
      throw new IllegalArgumentException("Input array length must be at least 2 * FFT length.");
    }
    if (length == 1) {
      return;
    }
    if (isPowerOfTwo(length)) {
      transform(values, length, false);
    } else {
      bluesteinForward(values);
    }
  }

  private void bluesteinForward(double[] values) {
    int convolutionLength = nextPowerOfTwo(2 * length - 1);
    double[] a = new double[2 * convolutionLength];
    double[] b = new double[2 * convolutionLength];

    for (int i = 0; i < length; i++) {
      double angle = Math.PI * i * (double) i / length;
      double cos = Math.cos(angle);
      double sin = Math.sin(angle);
      double real = values[2 * i];
      double imaginary = values[2 * i + 1];

      a[2 * i] = real * cos + imaginary * sin;
      a[2 * i + 1] = imaginary * cos - real * sin;
      b[2 * i] = cos;
      b[2 * i + 1] = sin;
      if (i > 0) {
        b[2 * (convolutionLength - i)] = cos;
        b[2 * (convolutionLength - i) + 1] = sin;
      }
    }

    transform(a, convolutionLength, false);
    transform(b, convolutionLength, false);
    for (int i = 0; i < convolutionLength; i++) {
      int offset = 2 * i;
      double real = a[offset] * b[offset] - a[offset + 1] * b[offset + 1];
      double imaginary = a[offset] * b[offset + 1] + a[offset + 1] * b[offset];
      a[offset] = real;
      a[offset + 1] = imaginary;
    }
    transform(a, convolutionLength, true);

    for (int i = 0; i < length; i++) {
      double angle = Math.PI * i * (double) i / length;
      double cos = Math.cos(angle);
      double sin = Math.sin(angle);
      double real = a[2 * i];
      double imaginary = a[2 * i + 1];
      values[2 * i] = real * cos + imaginary * sin;
      values[2 * i + 1] = imaginary * cos - real * sin;
    }
  }

  private static void transform(double[] values, int size, boolean inverse) {
    for (int i = 1, j = 0; i < size; i++) {
      int bit = size >>> 1;
      while ((j & bit) != 0) {
        j ^= bit;
        bit >>>= 1;
      }
      j ^= bit;
      if (i < j) {
        swap(values, 2 * i, 2 * j);
        swap(values, 2 * i + 1, 2 * j + 1);
      }
    }

    for (int step = 2; step <= size; step <<= 1) {
      double angle = (inverse ? 2.0 : -2.0) * Math.PI / step;
      double stepReal = Math.cos(angle);
      double stepImaginary = Math.sin(angle);
      int halfStep = step >>> 1;
      for (int block = 0; block < size; block += step) {
        double factorReal = 1.0;
        double factorImaginary = 0.0;
        for (int j = 0; j < halfStep; j++) {
          int even = 2 * (block + j);
          int odd = 2 * (block + j + halfStep);
          double oddReal = values[odd] * factorReal - values[odd + 1] * factorImaginary;
          double oddImaginary = values[odd] * factorImaginary + values[odd + 1] * factorReal;
          double evenReal = values[even];
          double evenImaginary = values[even + 1];

          values[even] = evenReal + oddReal;
          values[even + 1] = evenImaginary + oddImaginary;
          values[odd] = evenReal - oddReal;
          values[odd + 1] = evenImaginary - oddImaginary;

          double nextFactorReal = factorReal * stepReal - factorImaginary * stepImaginary;
          factorImaginary = factorReal * stepImaginary + factorImaginary * stepReal;
          factorReal = nextFactorReal;
        }
      }
    }

    if (inverse) {
      for (int i = 0; i < 2 * size; i++) {
        values[i] /= size;
      }
    }
  }

  private static boolean isPowerOfTwo(int value) {
    return (value & (value - 1)) == 0;
  }

  private static int nextPowerOfTwo(int value) {
    int highestOneBit = Integer.highestOneBit(value);
    return value == highestOneBit ? value : highestOneBit << 1;
  }

  private static void swap(double[] values, int left, int right) {
    double tmp = values[left];
    values[left] = values[right];
    values[right] = tmp;
  }
}

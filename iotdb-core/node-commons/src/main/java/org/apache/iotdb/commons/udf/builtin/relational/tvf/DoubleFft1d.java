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

package org.apache.iotdb.commons.udf.builtin.relational.tvf;

/**
 * A small complex FFT implementation for {@link FFTTableFunction}.
 *
 * <p>This class follows the {@code complexForward(double[])} contract of JTransforms 3.1 {@code
 * DoubleFFT_1D}, while keeping only the double-precision complex forward transform needed by
 * IoTDB's built-in FFT table function. The implementation uses radix-2 Cooley-Tukey for power of
 * two lengths and Bluestein convolution for other lengths.
 *
 * <p>Portions are derived from JTransforms 3.1, which carries the following notice:
 *
 * <pre>
 * JTransforms
 * Copyright (c) 2007 onward, Piotr Wendykier
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * </pre>
 */
final class DoubleFft1d {

  private final int length;
  private final int convolutionLength;
  private final double[] chirpCos;
  private final double[] chirpSin;
  private final double[] kernelSpectrum;

  DoubleFft1d(int length) {
    if (length < 1) {
      throw new IllegalArgumentException("FFT length must be positive.");
    }
    this.length = length;

    if (isPowerOfTwo(length)) {
      this.convolutionLength = 0;
      this.chirpCos = null;
      this.chirpSin = null;
      this.kernelSpectrum = null;
      return;
    }

    this.convolutionLength = nextPowerOfTwo(2 * length - 1);
    this.chirpCos = new double[length];
    this.chirpSin = new double[length];
    this.kernelSpectrum = new double[2 * convolutionLength];
    initializeBluesteinKernel();
  }

  void complexForward(double[] data) {
    if (data.length < 2 * length) {
      throw new IllegalArgumentException("The input array is too small for the FFT length.");
    }
    if (length == 1) {
      return;
    }
    if (isPowerOfTwo(length)) {
      transformRadix2(data, length, false);
    } else {
      transformBluestein(data);
    }
  }

  private void initializeBluesteinKernel() {
    long period = 2L * length;
    for (int i = 0; i < length; i++) {
      double angle = Math.PI * ((long) i * i % period) / length;
      double cos = Math.cos(angle);
      double sin = Math.sin(angle);
      chirpCos[i] = cos;
      chirpSin[i] = sin;
      kernelSpectrum[2 * i] = cos;
      kernelSpectrum[2 * i + 1] = sin;
      if (i > 0) {
        int mirroredIndex = 2 * (convolutionLength - i);
        kernelSpectrum[mirroredIndex] = cos;
        kernelSpectrum[mirroredIndex + 1] = sin;
      }
    }
    transformRadix2(kernelSpectrum, convolutionLength, false);
  }

  private void transformBluestein(double[] data) {
    double[] convolution = new double[2 * convolutionLength];
    for (int i = 0; i < length; i++) {
      double dataReal = data[2 * i];
      double dataImag = data[2 * i + 1];
      double cos = chirpCos[i];
      double sin = chirpSin[i];
      convolution[2 * i] = dataReal * cos + dataImag * sin;
      convolution[2 * i + 1] = dataImag * cos - dataReal * sin;
    }

    transformRadix2(convolution, convolutionLength, false);
    for (int i = 0; i < convolutionLength; i++) {
      int index = 2 * i;
      double real = convolution[index];
      double imag = convolution[index + 1];
      double kernelReal = kernelSpectrum[index];
      double kernelImag = kernelSpectrum[index + 1];
      convolution[index] = real * kernelReal - imag * kernelImag;
      convolution[index + 1] = real * kernelImag + imag * kernelReal;
    }
    transformRadix2(convolution, convolutionLength, true);

    for (int i = 0; i < length; i++) {
      double real = convolution[2 * i];
      double imag = convolution[2 * i + 1];
      double cos = chirpCos[i];
      double sin = chirpSin[i];
      data[2 * i] = real * cos + imag * sin;
      data[2 * i + 1] = imag * cos - real * sin;
    }
  }

  private static void transformRadix2(double[] data, int complexLength, boolean inverse) {
    bitReverse(data, complexLength);

    for (int size = 2; size <= complexLength; size <<= 1) {
      int halfSize = size >>> 1;
      double angle = (inverse ? 2.0 : -2.0) * Math.PI / size;
      double stepReal = Math.cos(angle);
      double stepImag = Math.sin(angle);

      for (int offset = 0; offset < complexLength; offset += size) {
        double twiddleReal = 1.0;
        double twiddleImag = 0.0;
        for (int i = 0; i < halfSize; i++) {
          int evenIndex = 2 * (offset + i);
          int oddIndex = 2 * (offset + i + halfSize);

          double oddReal = data[oddIndex];
          double oddImag = data[oddIndex + 1];
          double transformedOddReal = oddReal * twiddleReal - oddImag * twiddleImag;
          double transformedOddImag = oddReal * twiddleImag + oddImag * twiddleReal;

          double evenReal = data[evenIndex];
          double evenImag = data[evenIndex + 1];
          data[evenIndex] = evenReal + transformedOddReal;
          data[evenIndex + 1] = evenImag + transformedOddImag;
          data[oddIndex] = evenReal - transformedOddReal;
          data[oddIndex + 1] = evenImag - transformedOddImag;

          double nextTwiddleReal = twiddleReal * stepReal - twiddleImag * stepImag;
          twiddleImag = twiddleReal * stepImag + twiddleImag * stepReal;
          twiddleReal = nextTwiddleReal;
        }
      }
    }

    if (inverse) {
      for (int i = 0; i < 2 * complexLength; i++) {
        data[i] /= complexLength;
      }
    }
  }

  private static void bitReverse(double[] data, int complexLength) {
    int j = 0;
    for (int i = 1; i < complexLength; i++) {
      int bit = complexLength >>> 1;
      while ((j & bit) != 0) {
        j ^= bit;
        bit >>>= 1;
      }
      j ^= bit;
      if (i < j) {
        swap(data, 2 * i, 2 * j);
        swap(data, 2 * i + 1, 2 * j + 1);
      }
    }
  }

  private static void swap(double[] data, int left, int right) {
    double value = data[left];
    data[left] = data[right];
    data[right] = value;
  }

  private static boolean isPowerOfTwo(int value) {
    return (value & (value - 1)) == 0;
  }

  private static int nextPowerOfTwo(int value) {
    int highestOneBit = Integer.highestOneBit(value);
    return highestOneBit == value ? value : highestOneBit << 1;
  }
}

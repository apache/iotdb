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

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class FFT1DTest {

  @Test
  public void testDoubleComplexForwardPowerOfTwoLength() {
    double[] values = {1.0, 0.5, -2.0, 1.0, 0.0, -1.5, 3.0, 2.0};
    double[] expected = directDft(values);

    new DoubleFFT_1D(4).complexForward(values);

    assertArrayEquals(expected, values, 1e-9);
  }

  @Test
  public void testDoubleComplexForwardNonPowerOfTwoLength() {
    double[] values = {1.0, 0.0, 2.0, -0.5, -1.0, 1.5, 0.0, 0.25, 3.0, -2.0};
    double[] expected = directDft(values);

    new DoubleFFT_1D(5).complexForward(values);

    assertArrayEquals(expected, values, 1e-9);
  }

  @Test
  public void testDoubleComplexInversePowerOfTwoLength() {
    double[] values = {1.0, 0.5, -2.0, 1.0, 0.0, -1.5, 3.0, 2.0};
    double[] expected = values.clone();

    DoubleFFT_1D fft = new DoubleFFT_1D(4);
    fft.complexForward(values);
    fft.complexInverse(values, true);

    assertArrayEquals(expected, values, 1e-9);
  }

  @Test
  public void testDoubleComplexInverseNonPowerOfTwoLength() {
    double[] values = {1.0, 0.0, 2.0, -0.5, -1.0, 1.5, 0.0, 0.25, 3.0, -2.0};
    double[] expected = values.clone();

    DoubleFFT_1D fft = new DoubleFFT_1D(5);
    fft.complexForward(values);
    fft.complexInverse(values, true);

    assertArrayEquals(expected, values, 1e-9);
  }

  @Test
  public void testDoubleComplexInverseWithoutScaling() {
    double[] values = {1.0, 0.5, -2.0, 1.0, 0.0, -1.5, 3.0, 2.0};
    double[] expected = values.clone();
    for (int i = 0; i < expected.length; i++) {
      expected[i] *= 4;
    }

    DoubleFFT_1D fft = new DoubleFFT_1D(4);
    fft.complexForward(values);
    fft.complexInverse(values, false);

    assertArrayEquals(expected, values, 1e-9);
  }

  @Test
  public void testFloatComplexForwardPowerOfTwoLength() {
    float[] values = {1.0f, 0.5f, -2.0f, 1.0f, 0.0f, -1.5f, 3.0f, 2.0f};
    float[] expected = directDft(values);

    new FloatFFT_1D(4).complexForward(values);

    assertArrayEquals(expected, values, 1e-5f);
  }

  @Test
  public void testFloatComplexForwardNonPowerOfTwoLength() {
    float[] values = {1.0f, 0.0f, 2.0f, -0.5f, -1.0f, 1.5f, 0.0f, 0.25f, 3.0f, -2.0f};
    float[] expected = directDft(values);

    new FloatFFT_1D(5).complexForward(values);

    assertArrayEquals(expected, values, 1e-5f);
  }

  private static double[] directDft(double[] values) {
    int length = values.length / 2;
    double[] result = new double[values.length];
    for (int frequencyIndex = 0; frequencyIndex < length; frequencyIndex++) {
      double real = 0.0;
      double imaginary = 0.0;
      for (int timeIndex = 0; timeIndex < length; timeIndex++) {
        double angle = -2.0 * Math.PI * frequencyIndex * timeIndex / length;
        double cos = Math.cos(angle);
        double sin = Math.sin(angle);
        double inputReal = values[2 * timeIndex];
        double inputImaginary = values[2 * timeIndex + 1];
        real += inputReal * cos - inputImaginary * sin;
        imaginary += inputReal * sin + inputImaginary * cos;
      }
      result[2 * frequencyIndex] = real;
      result[2 * frequencyIndex + 1] = imaginary;
    }
    return result;
  }

  private static float[] directDft(float[] values) {
    int length = values.length / 2;
    float[] result = new float[values.length];
    for (int frequencyIndex = 0; frequencyIndex < length; frequencyIndex++) {
      double real = 0.0;
      double imaginary = 0.0;
      for (int timeIndex = 0; timeIndex < length; timeIndex++) {
        double angle = -2.0 * Math.PI * frequencyIndex * timeIndex / length;
        double cos = Math.cos(angle);
        double sin = Math.sin(angle);
        double inputReal = values[2 * timeIndex];
        double inputImaginary = values[2 * timeIndex + 1];
        real += inputReal * cos - inputImaginary * sin;
        imaginary += inputReal * sin + inputImaginary * cos;
      }
      result[2 * frequencyIndex] = (float) real;
      result[2 * frequencyIndex + 1] = (float) imaginary;
    }
    return result;
  }
}

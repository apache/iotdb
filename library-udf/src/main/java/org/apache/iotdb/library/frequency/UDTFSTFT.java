/*
 * Copyright © 2021 iotdb-quality developer group (iotdb-quality@protonmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.library.frequency;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.commons.math3.util.Pair;
import org.jtransforms.fft.DoubleFFT_1D;

/** This function does Short Time Fourier Transform for input series. */
public class UDTFSTFT implements UDTF {

  private int nfft;
  private int beta;
  private double snr;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validate(
            x -> (int) x > 0,
            "Nfft should be a positive integer.",
            validator.getParameters().getIntOrDefault("nfft", Integer.MAX_VALUE));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    this.nfft = parameters.getIntOrDefault("nfft", Integer.MAX_VALUE);
    this.beta = parameters.getIntOrDefault("beta", 0);
    this.snr = parameters.getDoubleOrDefault("T_SNR", Double.NaN);
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(nfft))
        .setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    int n = rowWindow.windowSize();
    DoubleFFT_1D fft = new DoubleFFT_1D(n);
    double[] a = new double[2 * n];
    for (int i = 0; i < n; i++) {
      a[2 * i] = Util.getValueAsDouble(rowWindow.getRow(i));
      a[2 * i + 1] = 0;
    }
    fft.complexForward(a);
    double b[] = new double[n];
    for (int i = 0; i < n; i++) {
      double v = Math.sqrt(a[2 * i] * a[2 * i] + a[2 * i + 1] * a[2 * i + 1]);
      v /= n;
      b[i] = v;
    }
    if (Double.isNaN(this.snr)) {
      double eps = Math.pow(2, beta);
      for (int i = 0; i < n; i++) {
        b[i] = Math.round(b[i] / eps) * eps;
      }
    } else {
      b = quantize(b);
    }
    for (int i = 0; i < n; i++) {
      collector.putDouble(rowWindow.getRow(i).getTime(), b[i]);
    }
  }

  private double[] quantize(double a[]) {
    int beta = initBeta(a);
    while (true) {
      Pair<Integer, Double> p = quantizeWithBeta(a, beta);
      if (p.getSecond() < this.snr) {
        break;
      }
      beta++;
    }
    beta--;
    double eps = Math.pow(2, beta);
    double[] b = new double[a.length];
    for (int i = 0; i < a.length; i++) {
      b[i] = (long) Math.round(a[i] / eps) * eps;
    }
    return b;
  }

  private int initBeta(double a[]) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      sum += a[i] * a[i];
    }
    sum = sum * 1e-6 / a.length;
    return (int) (Math.floor(0.5 * Math.log(sum) / Math.log(2)) + 1);
  }

  private Pair<Integer, Double> quantizeWithBeta(double a[], int beta) {
    double eps = Math.pow(2, beta);
    int cnt = 0;
    double noise = 0, signal = 0;
    for (int i = 0; i < a.length; i++) {
      int t = (int) Math.round(a[i] / eps);
      double x = a[i] - t * eps;
      if (t > 0) {
        cnt++;
      }
      signal += a[i] * a[i];
      noise += x * x;
    }
    double snr = 10 * Math.log10(signal / noise);
    return Pair.create(cnt, snr);
  }
}

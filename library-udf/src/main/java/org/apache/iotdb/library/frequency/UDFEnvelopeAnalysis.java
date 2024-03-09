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

package org.apache.iotdb.library.frequency;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFOutputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.commons.math3.complex.Complex;
import org.jtransforms.fft.DoubleFFT_1D;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UDFEnvelopeAnalysis implements UDTF {
  private int frequency;

  private final List<Double> list = new ArrayList<>();

  public static final String frequencyConstant = "frequency";

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateRequiredAttribute(frequencyConstant)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validate(
            x -> validator.getParameters().getAttributes().size() == 1,
            "The 'envelope' function takes only 'frequency' as an argument.",
            validator.getParameters());
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    this.frequency = Integer.parseInt(parameters.getStringOrDefault(frequencyConstant, "0"));
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double valueAsDouble = getValueAsDouble(row, 0);
    list.add(valueAsDouble);
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    int signalSize = list.size();
    double[] signals = list.stream().mapToDouble(Double::doubleValue).toArray();
    double[] envelope = envelope(signals);
    double[] f = new double[signalSize / 2];
    for (int i = 0; i < signalSize / 2; i++) {
      f[i] = i * ((double) frequency / signalSize);
    }

    for (int i = 0; i < envelope.length; i++) {
      collector.putDouble((long) f[i], envelope[i]);
    }
  }

  public Complex[] hilbert(double[] timeDomainSignal) {
    int signalLength = timeDomainSignal.length;
    // 1. FFT transformer setup
    DoubleFFT_1D fftTransformer = new DoubleFFT_1D(signalLength);
    // 2. array for FFT output (real + imaginary)
    double[] frequencyDomain = new double[signalLength * 2];
    // 3. copy signal into FFT input array
    System.arraycopy(timeDomainSignal, 0, frequencyDomain, 0, signalLength);
    // 4. forward FFT to get frequency representation
    fftTransformer.realForwardFull(frequencyDomain);

    // 5. hilbert filter to zero negative frequencies and double positive ones
    double[] hilbertFilter = new double[signalLength];
    // 6. adjust filter for DC and Nyquist when signalLength is even
    if (signalLength % 2 == 0) {
      hilbertFilter[0] = hilbertFilter[signalLength / 2] = 1;
      Arrays.fill(hilbertFilter, 1, signalLength / 2, 2);
    } else {
      // adjust filter for DC when signalLength is odd
      hilbertFilter[0] = 1;
      Arrays.fill(hilbertFilter, 1, (signalLength + 1) / 2, 2);
    }

    // 7. apply Hilbert filter
    for (int i = 0; i < signalLength; i++) {
      frequencyDomain[2 * i] *= hilbertFilter[i];
      frequencyDomain[2 * i + 1] *= hilbertFilter[i];
    }

    // 8. inverse FFT to time domain
    fftTransformer.complexInverse(frequencyDomain, true);

    // 9. form analytic signal
    Complex[] analyticSignal = new Complex[signalLength];
    for (int i = 0; i < signalLength; i++) {
      analyticSignal[i] = new Complex(frequencyDomain[2 * i], frequencyDomain[2 * i + 1]);
    }

    return analyticSignal;
  }

  public double[] envelope(double[] signals) {
    Complex[] complexes = hilbert(signals);
    double[] result = new double[complexes.length];
    for (int i = 0; i < complexes.length; i++) {
      result[i] =
          Math.abs(
              Math.sqrt(
                  complexes[i].getReal() * complexes[i].getReal()
                      + complexes[i].getImaginary() * complexes[i].getImaginary()));
    }
    DoubleFFT_1D fftDo = new DoubleFFT_1D(result.length);
    double[] fft = new double[result.length * 2];
    System.arraycopy(result, 0, fft, 0, result.length);
    fftDo.realForwardFull(fft);

    double[] env_spec_x = new double[signals.length / 2];
    for (int i = 0; i < signals.length / 2; i++) {
      env_spec_x[i] =
          Math.abs(Math.sqrt(fft[2 * i] * fft[2 * i] + fft[2 * i + 1] * fft[2 * i + 1]))
              / signals.length;
    }
    return env_spec_x;
  }

  public double getValueAsDouble(Row row, int index) throws IOException {
    double ans = 0;
    try {
      switch (row.getDataType(index)) {
        case INT32:
          ans = row.getInt(index);
          break;
        case INT64:
          ans = row.getLong(index);
          break;
        case FLOAT:
          ans = row.getFloat(index);
          break;
        case DOUBLE:
          ans = row.getDouble(index);
          break;
      }
    } catch (IOException e) {
      throw new UDFOutputSeriesDataTypeNotValidException(
          index, "Fail to get data type in row " + row.getTime());
    }
    return ans;
  }
}

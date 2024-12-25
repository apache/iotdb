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
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.jtransforms.fft.DoubleFFT_1D;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class UDFEnvelopeAnalysis implements UDTF {
  private double frequency;
  private int amplification;
  private String timestampPrecision;
  private final DoubleArrayList signals = new DoubleArrayList();
  private final LongArrayList timestamps = new LongArrayList();
  private static final String TIMESTAMP_PRECISION = "timestampPrecision";
  private static final String FREQUENCY = "frequency";
  private static final String AMPLIFICATION = "amplification";
  public static final String MS_PRECISION = "ms";
  public static final String US_PRECISION = "us";
  public static final String NS_PRECISION = "ns";

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validate(
            x -> (double) x > 0,
            "The param 'frequency' must > 0.",
            validator.getParameters().getDoubleOrDefault(FREQUENCY, Double.MAX_VALUE))
        .validate(
            x -> (int) x >= 1,
            "The param 'amplification' must >= 1.",
            validator.getParameters().getIntOrDefault(AMPLIFICATION, 1));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    frequency = parameters.getDoubleOrDefault(FREQUENCY, Double.MAX_VALUE);
    amplification = parameters.getIntOrDefault(AMPLIFICATION, 1);
    timestampPrecision = parameters.getSystemStringOrDefault(TIMESTAMP_PRECISION, MS_PRECISION);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    signals.add(getValueAsDouble(row, 0));
    if (timestamps.size() < 10) {
      timestamps.add(row.getTime());
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    double[] envelopeValues = envelopeAnalyze(signals.toArray());
    frequency = frequency != Double.MAX_VALUE ? frequency : calculateFrequency(timestamps);
    int signalSize = signals.size();
    double[] frequencies = new double[signalSize / 2];
    for (int i = 0; i < signalSize / 2; i++) {
      frequencies[i] = i * (frequency * amplification / signalSize);
    }

    for (int i = 0; i < envelopeValues.length; i++) {
      collector.putDouble((long) frequencies[i], envelopeValues[i]);
    }
  }

  public double[] envelopeAnalyze(double[] signals) {
    Complex[] hilbertTransformed = calculateHilbert(signals);
    double[] hilbertAbs = calculateAbs(hilbertTransformed);
    double[] fftTransformed = calculateFFT(hilbertAbs);
    return calculateEnvelope(signals.length, fftTransformed);
  }

  public Complex[] calculateHilbert(double[] timeDomainSignal) {
    int signalSize = timeDomainSignal.length;
    // 1. FFT transformer setup
    DoubleFFT_1D fftTransformer = new DoubleFFT_1D(signalSize);

    // 2. array for FFT output (real + imaginary)
    double[] frequencyDomainValues = new double[signalSize * 2];

    // 3. copy signal into FFT input array
    System.arraycopy(timeDomainSignal, 0, frequencyDomainValues, 0, signalSize);

    // 4. forward FFT to get frequency representation
    fftTransformer.realForwardFull(frequencyDomainValues);

    // 5. hilbert filter to zero negative frequencies and double positive ones, and adjust filter
    // for DC and Nyquist when signalSize is even
    double[] hilbertFilter = new double[signalSize];
    if (signalSize % 2 == 0) {
      hilbertFilter[0] = hilbertFilter[signalSize / 2] = 1;
      Arrays.fill(hilbertFilter, 1, signalSize / 2, 2);
    } else {
      // adjust filter for DC when signalSize is odd
      hilbertFilter[0] = 1;
      Arrays.fill(hilbertFilter, 1, (signalSize + 1) / 2, 2);
    }

    // 6. apply Hilbert filter
    for (int i = 0; i < signalSize; i++) {
      frequencyDomainValues[2 * i] *= hilbertFilter[i];
      frequencyDomainValues[2 * i + 1] *= hilbertFilter[i];
    }

    // 7. inverse FFT to time domain
    fftTransformer.complexInverse(frequencyDomainValues, true);

    // 8. form analytic signal
    Complex[] analyticSignals = new Complex[signalSize];
    for (int i = 0; i < signalSize; i++) {
      analyticSignals[i] =
          new Complex(frequencyDomainValues[2 * i], frequencyDomainValues[2 * i + 1]);
    }

    return analyticSignals;
  }

  private double[] calculateAbs(Complex[] complexNumbers) {
    double[] magnitudes = new double[complexNumbers.length];
    for (int i = 0; i < complexNumbers.length; i++) {
      magnitudes[i] = complexNumbers[i].abs();
    }
    return magnitudes;
  }

  private double[] calculateFFT(double[] realValues) {
    DoubleFFT_1D fftTransformer = new DoubleFFT_1D(realValues.length);
    double[] fftComplex = new double[realValues.length * 2];
    System.arraycopy(realValues, 0, fftComplex, 0, realValues.length);
    fftTransformer.realForwardFull(fftComplex);
    return fftComplex;
  }

  private double[] calculateEnvelope(int originalLength, double[] fftValues) {
    double[] envelope = new double[originalLength / 2];
    for (int i = 0; i < envelope.length; i++) {
      int realIndex = 2 * i;
      int imagIndex = realIndex + 1;
      envelope[i] =
          Math.sqrt(
                  fftValues[realIndex] * fftValues[realIndex]
                      + fftValues[imagIndex] * fftValues[imagIndex])
              / originalLength;
    }
    return envelope;
  }

  public double calculateFrequency(LongArrayList timestamps) {
    LongArrayList timeDifferences = calculateTimeDifferences(timestamps);
    long modeTimeDifference = calculateMode(timeDifferences);
    return calculateFrequencyByTimeUnit(modeTimeDifference, timestampPrecision);
  }

  public LongArrayList calculateTimeDifferences(LongArrayList timestamps) {
    LongArrayList timeDifferences = new LongArrayList();
    for (int i = 1; i < timestamps.size(); i++) {
      timeDifferences.add(timestamps.get(i) - timestamps.get(i - 1));
    }
    return timeDifferences;
  }

  public long calculateMode(LongArrayList timestamps) {
    Map<Long, Integer> countMap = new HashMap<>();
    int maxCount = 0;
    long modeTimeDifference = 0L;

    for (long diff : timestamps.toArray()) {
      int count = countMap.getOrDefault(diff, 0) + 1;
      countMap.put(diff, count);
      if (count > maxCount) {
        maxCount = count;
        modeTimeDifference = diff;
      }
    }
    return modeTimeDifference;
  }

  public double getValueAsDouble(Row row, int index) throws IOException {
    double ans;
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
      case TEXT:
      case STRING:
      case BOOLEAN:
      case BLOB:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UDFOutputSeriesDataTypeNotValidException(
            index, "Fail to get data type in row " + row.getTime());
    }
    return ans;
  }

  public static double calculateFrequencyByTimeUnit(long time, String timeUnit) {
    switch (timeUnit) {
      case MS_PRECISION:
        return 1000.0 / time;
      case US_PRECISION:
        return 1_000_000.0 / time;
      case NS_PRECISION:
        return 1_000_000_000.0 / time;
      default:
        throw new IllegalArgumentException("Unsupported time unit.");
    }
  }
}

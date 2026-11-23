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

package org.apache.iotdb.library.anomaly;

import org.apache.iotdb.library.i18n.LibraryUdfMessages;
import org.apache.iotdb.library.util.CircularQueue;
import org.apache.iotdb.library.util.LongCircularQueue;
import org.apache.iotdb.library.util.TypeServices;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayDeque;
import java.util.Deque;

/** This function detects outliers which lies over average +/- k * sigma. */
public class UDTFKSigma implements UDTF {
  private static final int DEFAULT_WINDOW_SIZE = 10000;

  private double mean = 0.0;
  private double sumSquaredDeviation = 0.0;
  private double safeMomentMagnitude;
  private double statisticScale;
  private double multipleK;
  private boolean usesScaledStatistics;
  private final Deque<MagnitudeEntry> maximumMagnitudes = new ArrayDeque<>();
  private long nextValueSequence;
  private int windowSize = 0;
  private CircularQueue<Object> v;
  private LongCircularQueue t;
  private LongCircularQueue valueSequences;
  private Type dataType;
  private TypeServices.RowValueReader valueReader;
  private TypeServices.NumericRowReader numericReader;
  private TypeServices.RowValueWriter valueWriter;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            x -> (int) x > 0,
            LibraryUdfMessages.EXCEPTION_WINDOW_SIZE_SHOULD_BE_LARGER_THAN_0_F4C2184C,
            validator.getParameters().getIntOrDefault("window", DEFAULT_WINDOW_SIZE))
        .validate(
            x -> Double.isFinite((double) x) && (double) x > 0,
            LibraryUdfMessages.EXCEPTION_PARAMETER_K_SHOULD_BE_FINITE_AND_LARGER_THAN_0_ECE1B163,
            validator.getParameters().getDoubleOrDefault("k", 3));
  }

  @Override
  public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations)
      throws Exception {
    udtfConfigurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(udfParameters.getDataType(0));
    this.multipleK = udfParameters.getDoubleOrDefault("k", 3);
    this.dataType = udfParameters.getDataType(0);
    valueReader = TypeServices.ROW_VALUE_READER_SERVICE.call(TypeServices.toReadType(dataType));
    numericReader = TypeServices.NUMERIC_ROW_READER_SERVICE.call(TypeServices.toReadType(dataType));
    valueWriter = TypeServices.rowValueWriter(dataType);
    this.windowSize = udfParameters.getIntOrDefault("window", DEFAULT_WINDOW_SIZE);
    this.mean = 0.0;
    this.sumSquaredDeviation = 0.0;
    this.safeMomentMagnitude = Math.sqrt(Double.MAX_VALUE / (8.0 * windowSize));
    this.statisticScale = 0.0;
    this.usesScaledStatistics = false;
    this.maximumMagnitudes.clear();
    this.nextValueSequence = 0;
    this.v = new CircularQueue<>(windowSize);
    this.t = new LongCircularQueue(windowSize);
    this.valueSequences = new LongCircularQueue(windowSize);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0)) {
      return;
    }
    double value = numericReader.read(row);
    long timestamp = row.getTime();
    if (Double.isFinite(value) && !Double.isNaN(value)) {
      long sequence = nextValueSequence++;
      if (v.isFull()) {
        final double frontValue = ((Number) v.pop()).doubleValue();
        t.pop();
        removeValue(frontValue, valueSequences.pop());
        Object storedValue = valueReader.read(row);
        v.push(storedValue);
        t.push(timestamp);
        valueSequences.push(sequence);
        addValue(value, sequence);
        if (v.getSize() > 1 && isOutlier(value)) {
          valueWriter.write(collector, timestamp, storedValue);
        }
      } else {
        Object storedValue = valueReader.read(row);
        v.push(storedValue);
        t.push(timestamp);
        valueSequences.push(sequence);
        addValue(value, sequence);
        if (v.getSize() == this.windowSize) {
          for (int i = 0; i < v.getSize(); i++) {
            Object windowValue = this.v.get(i);
            timestamp = this.t.get(i);
            if (isOutlier(((Number) windowValue).doubleValue())) {
              valueWriter.write(collector, timestamp, windowValue);
            }
          }
        }
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (!v.isFull() && v.getSize() > 1) {
      for (int i = 0; i < v.getSize(); i++) {
        Object windowValue = this.v.get(i);
        long timestamp = this.t.get(i);
        if (isOutlier(((Number) windowValue).doubleValue())) {
          valueWriter.write(collector, timestamp, windowValue);
        }
      }
    }
  }

  private void addValue(double value, long sequence) {
    double magnitude = Math.abs(value);
    while (!maximumMagnitudes.isEmpty() && maximumMagnitudes.peekLast().magnitude <= magnitude) {
      maximumMagnitudes.removeLast();
    }
    maximumMagnitudes.addLast(new MagnitudeEntry(sequence, magnitude));
    if (usesScaledStatistics) {
      double newScale = maximumMagnitudes.peekFirst().magnitude;
      if (newScale <= safeMomentMagnitude) {
        recomputeStatistics();
        return;
      }
      rescaleStatistics(newScale);
      addScaledValue(value / statisticScale);
      return;
    }
    if (magnitude > safeMomentMagnitude) {
      recomputeScaledStatistics(magnitude);
      return;
    }
    int newCount = v.getSize();
    double delta = value - mean;
    mean += delta / newCount;
    sumSquaredDeviation += delta * (value - mean);
    if (!Double.isFinite(mean) || !Double.isFinite(sumSquaredDeviation)) {
      recomputeStatistics();
    }
  }

  private void removeValue(double value, long sequence) {
    if (!maximumMagnitudes.isEmpty() && maximumMagnitudes.peekFirst().sequence == sequence) {
      maximumMagnitudes.removeFirst();
    }
    if (usesScaledStatistics) {
      int remainingCount = v.getSize();
      if (remainingCount == 0) {
        mean = 0;
        sumSquaredDeviation = 0;
        statisticScale = 0;
        usesScaledStatistics = false;
        return;
      }
      double comparedValue = value / statisticScale;
      double updatedMean = mean - (comparedValue - mean) / remainingCount;
      sumSquaredDeviation -= (comparedValue - mean) * (comparedValue - updatedMean);
      mean = updatedMean;
      if (sumSquaredDeviation < 0 || !Double.isFinite(sumSquaredDeviation)) {
        recomputeStatistics();
      }
      return;
    }
    int remainingCount = v.getSize();
    if (remainingCount == 0) {
      mean = 0;
      sumSquaredDeviation = 0;
      statisticScale = 0;
      return;
    }
    double delta = value - mean;
    double updatedMean = mean - delta / remainingCount;
    sumSquaredDeviation -= delta * (value - updatedMean);
    mean = updatedMean;
    if (sumSquaredDeviation < 0 || !Double.isFinite(sumSquaredDeviation)) {
      recomputeStatistics();
    }
  }

  private void recomputeStatistics() {
    double maxMagnitude = maximumMagnitudes.isEmpty() ? 0 : maximumMagnitudes.peekFirst().magnitude;
    if (maxMagnitude > safeMomentMagnitude) {
      recomputeScaledStatistics(maxMagnitude);
      return;
    }

    usesScaledStatistics = false;
    statisticScale = 0;
    mean = 0;
    sumSquaredDeviation = 0;
    for (int i = 0; i < v.getSize(); i++) {
      double value = ((Number) v.get(i)).doubleValue();
      int count = i + 1;
      double delta = value - mean;
      mean += delta / count;
      sumSquaredDeviation += delta * (value - mean);
      if (!Double.isFinite(mean) || !Double.isFinite(sumSquaredDeviation)) {
        recomputeScaledStatistics(maxMagnitude);
        return;
      }
    }
  }

  private void recomputeScaledStatistics(double scale) {
    mean = 0;
    sumSquaredDeviation = 0;
    statisticScale = scale;
    usesScaledStatistics = scale > 0;
    if (!usesScaledStatistics) {
      return;
    }

    for (int i = 0; i < v.getSize(); i++) {
      double value = ((Number) v.get(i)).doubleValue() / scale;
      int count = i + 1;
      double delta = value - mean;
      mean += delta / count;
      sumSquaredDeviation += delta * (value - mean);
    }
  }

  private void addScaledValue(double value) {
    int newCount = v.getSize();
    double delta = value - mean;
    mean += delta / newCount;
    sumSquaredDeviation += delta * (value - mean);
  }

  private void rescaleStatistics(double newScale) {
    if (newScale == statisticScale) {
      return;
    }
    double ratio = statisticScale / newScale;
    mean *= ratio;
    sumSquaredDeviation *= ratio * ratio;
    statisticScale = newScale;
  }

  private static class MagnitudeEntry {
    private final long sequence;
    private final double magnitude;

    private MagnitudeEntry(long sequence, double magnitude) {
      this.sequence = sequence;
      this.magnitude = magnitude;
    }
  }

  private boolean isOutlier(double value) {
    double comparedValue = usesScaledStatistics ? value / statisticScale : value;
    return Math.abs(comparedValue - mean) > multipleK * sampleStandardDeviation();
  }

  private double sampleStandardDeviation() {
    return Math.sqrt(Math.max(0, sumSquaredDeviation) / (v.getSize() - 1));
  }
}

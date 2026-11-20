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

/** This function detects outliers which lies over average +/- k * sigma. */
public class UDTFKSigma implements UDTF {
  private static final int DEFAULT_WINDOW_SIZE = 10000;

  private double mean = 0.0;
  private double sumSquaredDeviation = 0.0;
  private double multipleK;
  private int windowSize = 0;
  private CircularQueue<Object> v;
  private LongCircularQueue t;
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
    this.v = new CircularQueue<>(windowSize);
    this.t = new LongCircularQueue(windowSize);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0)) {
      return;
    }
    double value = numericReader.read(row);
    long timestamp = row.getTime();
    if (Double.isFinite(value) && !Double.isNaN(value)) {
      if (v.isFull()) {
        final double frontValue = Double.parseDouble(v.pop().toString());
        t.pop();
        removeValue(frontValue);
        Object storedValue = valueReader.read(row);
        v.push(storedValue);
        t.push(timestamp);
        addValue(value);
        if (v.getSize() > 1 && Math.abs(value - mean) > multipleK * sampleStandardDeviation()) {
          valueWriter.write(collector, timestamp, storedValue);
        }
      } else {
        Object storedValue = valueReader.read(row);
        v.push(storedValue);
        t.push(timestamp);
        addValue(value);
        if (v.getSize() == this.windowSize) {
          double standardDeviation = sampleStandardDeviation();
          for (int i = 0; i < v.getSize(); i++) {
            Object windowValue = this.v.get(i);
            timestamp = this.t.get(i);
            if (Math.abs(Double.parseDouble(windowValue.toString()) - mean)
                > multipleK * standardDeviation) {
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
      double standardDeviation = sampleStandardDeviation();
      for (int i = 0; i < v.getSize(); i++) {
        Object windowValue = this.v.get(i);
        long timestamp = this.t.get(i);
        if (Math.abs(Double.parseDouble(windowValue.toString()) - mean)
            > multipleK * standardDeviation) {
          valueWriter.write(collector, timestamp, windowValue);
        }
      }
    }
  }

  private void addValue(double value) {
    int newCount = v.getSize();
    double delta = value - mean;
    mean += delta / newCount;
    sumSquaredDeviation += delta * (value - mean);
  }

  private void removeValue(double value) {
    int remainingCount = v.getSize();
    if (remainingCount == 0) {
      mean = 0;
      sumSquaredDeviation = 0;
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
    mean = 0;
    sumSquaredDeviation = 0;
    for (int i = 0; i < v.getSize(); i++) {
      double value = Double.parseDouble(v.get(i).toString());
      int count = i + 1;
      double delta = value - mean;
      mean += delta / count;
      sumSquaredDeviation += delta * (value - mean);
    }
  }

  private double sampleStandardDeviation() {
    return Math.sqrt(Math.max(0, sumSquaredDeviation) / (v.getSize() - 1));
  }
}

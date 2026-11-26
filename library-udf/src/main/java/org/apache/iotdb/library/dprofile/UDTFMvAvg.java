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

package org.apache.iotdb.library.dprofile;

import org.apache.iotdb.library.i18n.LibraryUdfMessages;
import org.apache.iotdb.library.util.DoubleCircularQueue;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/** This function calculates moving average of given window length of input series. */
public class UDTFMvAvg implements UDTF {
  int windowSize;
  Type dataType;
  DoubleCircularQueue v;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            x -> (int) x > 0,
            LibraryUdfMessages.EXCEPTION_WINDOW_SIZE_SHOULD_BE_LARGER_THAN_0_F4C2184C,
            validator.getParameters().getIntOrDefault("window", 10));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    dataType = parameters.getDataType(0);
    windowSize = parameters.getIntOrDefault("window", 10);
    v = new DoubleCircularQueue(windowSize);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    long t = row.getTime();
    if (v.isFull()) {
      v.pop();
    }
    double value = Util.getValueAsDouble(row);
    if (Double.isFinite(value)) {
      v.push(value);
      if (v.isFull()) {
        collector.putDouble(t, windowMean());
      }
    }
  }

  /**
   * Mean of the values that are currently in the window.
   *
   * <p>The sum is recomputed from the retained values instead of being carried as a running double:
   * a running sum leaks the rounding error of already evicted values into later windows (adding
   * {@code 1} to {@code 2^53} loses the unit, and evicting that {@code 1} still subtracts it, so
   * following windows drift and can even become negative). Recomputing has no such state, and
   * Neumaier compensation removes the rounding error of the summation itself.
   *
   * <p>The two compensated parts are divided before being added, which keeps the result finite even
   * when {@code sum + compensation} itself would overflow. If the plain window sum still overflows
   * to Infinity (e.g. two {@code Double.MAX_VALUE} values), the mean is accumulated on values
   * pre-divided by the window size; that cannot overflow and keeps later windows exact.
   */
  private double windowMean() {
    int size = v.getSize();
    double sum = 0.0d;
    double compensation = 0.0d;
    for (int i = 0; i < size; i++) {
      double value = v.get(i);
      double next = sum + value;
      if (Math.abs(sum) >= Math.abs(value)) {
        compensation += (sum - next) + value;
      } else {
        compensation += (value - next) + sum;
      }
      sum = next;
    }
    if (Double.isFinite(sum)) {
      return sum / size + compensation / size;
    }
    // the window sum overflows although every value is finite: scale before summing so that
    // neither the partial sums nor the result can overflow
    double scaledSum = 0.0d;
    double scaledCompensation = 0.0d;
    for (int i = 0; i < size; i++) {
      double value = v.get(i) / size;
      double next = scaledSum + value;
      if (Math.abs(scaledSum) >= Math.abs(value)) {
        scaledCompensation += (scaledSum - next) + value;
      } else {
        scaledCompensation += (value - next) + scaledSum;
      }
      scaledSum = next;
    }
    return scaledSum + scaledCompensation;
  }

  @Override
  public void terminate(PointCollector collector)
      throws Exception { // default implementation ignored
  }
}

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

import java.math.BigDecimal;

/** This function calculates moving average of given window length of input series. */
public class UDTFMvAvg implements UDTF {
  int windowSize;
  Type dataType;
  DoubleCircularQueue v;
  double windowSum;
  double windowCompensation;
  private BigDecimal exactWindowSum;

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
    windowSum = 0d;
    windowCompensation = 0d;
    exactWindowSum = null;
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0)) {
      return;
    }
    double value = Util.getValueAsDouble(row);
    if (Double.isFinite(value)) {
      boolean rebuild = false;
      if (v.isFull()) {
        double removed = v.pop();
        if (exactWindowSum == null) {
          rebuild = addToSum(-removed);
        } else {
          exactWindowSum = exactWindowSum.subtract(new BigDecimal(removed));
        }
      }
      v.push(value);
      if (exactWindowSum == null) {
        rebuild |= addToSum(value);
        if (rebuild) {
          // A single compensation term cannot retain every scale of a finite input window.
          // Rebuild once, then keep an exact sum so later removals recover the small values.
          exactWindowSum = BigDecimal.ZERO;
          for (int i = 0; i < v.getSize(); i++) {
            exactWindowSum = exactWindowSum.add(new BigDecimal(v.get(i)));
          }
        }
      } else {
        exactWindowSum = exactWindowSum.add(new BigDecimal(value));
      }
      if (v.isFull()) {
        double sum =
            exactWindowSum == null ? windowSum + windowCompensation : exactWindowSum.doubleValue();
        double average = sum / windowSize;
        if (Double.isFinite(average)) {
          collector.putDouble(row.getTime(), average);
        }
      }
    }
  }

  private boolean addToSum(double value) {
    double sum = windowSum + value;
    if (!Double.isFinite(sum)) {
      return true;
    }
    // Retain small values until larger values leave the window (Neumaier summation).
    double correction =
        Math.abs(windowSum) >= Math.abs(value)
            ? (windowSum - sum) + value
            : (value - sum) + windowSum;
    double compensation = windowCompensation + correction;
    double lost =
        Math.abs(windowCompensation) >= Math.abs(correction)
            ? (windowCompensation - compensation) + correction
            : (correction - compensation) + windowCompensation;
    windowSum = sum;
    windowCompensation = compensation;
    return !Double.isFinite(compensation) || lost != 0d;
  }

  @Override
  public void terminate(PointCollector collector)
      throws Exception { // default implementation ignored
  }
}

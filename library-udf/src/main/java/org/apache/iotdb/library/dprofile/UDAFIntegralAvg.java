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
import java.math.MathContext;

/**
 * calculate the integral or the area under the curve of input series $unit$ is the time scale for
 * the area calculation, chosen from 1s(second, default), 1m(minute), 1h(hour), 1d(day).
 */
public class UDAFIntegralAvg implements UDTF {

  long startTime = -1;
  long lastTime = -1;
  boolean hasValue;
  double lastValue = 0;
  BigDecimal integralValue = BigDecimal.ZERO;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    startTime = -1;
    lastTime = -1;
    hasValue = false;
    lastValue = 0;
    integralValue = BigDecimal.ZERO;
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0)) {
      return;
    }
    long nowTime = row.getTime();
    double nowValue = Util.getValueAsDouble(row);
    if (Double.isFinite(nowValue)) {
      if (!hasValue) {
        startTime = nowTime;
      } else {
        // calculate the ladder-shaped area between last point and this one
        // Normalize before conversion to double so extreme areas retain a finite average.
        BigDecimal timeSpan = BigDecimal.valueOf(nowTime).subtract(BigDecimal.valueOf(lastTime));
        BigDecimal area =
            new BigDecimal(lastValue)
                .add(new BigDecimal(nowValue))
                .multiply(timeSpan)
                .divide(BigDecimal.valueOf(2));
        integralValue = integralValue.add(area);
      }
      lastTime = nowTime;
      lastValue = nowValue;
      hasValue = true;
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (!hasValue) {
      // empty input
      collector.putDouble(0, 0);
    } else if (startTime == lastTime) {
      // one single point
      collector.putDouble(0, lastValue);
    } else {
      BigDecimal timeSpan = BigDecimal.valueOf(lastTime).subtract(BigDecimal.valueOf(startTime));
      collector.putDouble(0, integralValue.divide(timeSpan, MathContext.DECIMAL128).doubleValue());
    }
  }
}

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

/**
 * calculate the integral or the area under the curve of input series $unit$ is the time scale for
 * the area calculation, chosen from 1s(second, default), 1m(minute), 1h(hour), 1d(day)
 */
public class UDAFIntegral implements UDTF {

  private static final String TIME_UNIT_KEY = "unit";
  private static final String TIME_UNIT_MS = "1S";
  private static final String TIME_UNIT_S = "1s";
  private static final String TIME_UNIT_M = "1m";
  private static final String TIME_UNIT_H = "1H";
  private static final String TIME_UNIT_D = "1d";

  long unitTime;
  long lastTime = -1;
  double lastValue;
  double integralValue = 0;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validate(
            unit ->
                TIME_UNIT_D.equals(unit)
                    || TIME_UNIT_H.equals(unit)
                    || TIME_UNIT_M.equals(unit)
                    || TIME_UNIT_S.equals(unit)
                    || TIME_UNIT_MS.equals(unit),
            "Unknown time unit input",
            validator.getParameters().getStringOrDefault(TIME_UNIT_KEY, TIME_UNIT_S));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    switch (parameters.getStringOrDefault(TIME_UNIT_KEY, TIME_UNIT_S)) {
      case TIME_UNIT_MS:
        unitTime = 1L;
        break;
      case TIME_UNIT_S:
        unitTime = 1000L;
        break;
      case TIME_UNIT_M:
        unitTime = 60000L;
        break;
      case TIME_UNIT_H:
        unitTime = 3600000L;
        break;
      case TIME_UNIT_D:
        unitTime = 3600000L * 24L;
        break;
      default:
        throw new Exception(
            "Unknown time unit input: "
                + parameters.getStringOrDefault(TIME_UNIT_KEY, TIME_UNIT_S));
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    long nowTime = row.getTime();
    double nowValue = Util.getValueAsDouble(row);
    if (Double.isFinite(nowValue)) {
      // calculate the ladder-shaped area between last point and this one
      // skip and initialize the memory if no existing previous point is available
      if (lastTime >= 0) {
        integralValue += (lastValue + nowValue) * (nowTime - lastTime) / 2.0 / unitTime;
      }
      lastTime = nowTime;
      lastValue = nowValue;
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    collector.putDouble(0, integralValue); // default: 0
  }
}

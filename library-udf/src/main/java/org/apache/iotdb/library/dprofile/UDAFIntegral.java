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
 * the area calculation, chosen from 1s(second, default), 1m(minute), 1h(hour), 1d(day).
 */
public class UDAFIntegral implements UDTF {

  private static final String TIME_UNIT_KEY = "unit";
  private static final String TIME_UNIT_S = "1s";

  long unitTime;
  long lastTime = -1;
  double lastValue;
  double integralValue = 0;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validate(
            x -> (long) x > 0,
            "Unknown time unit input. Supported units are ns, us, ms, s, m, h, d.",
            Util.parseTime(
                validator.getParameters().getStringOrDefault(TIME_UNIT_KEY, TIME_UNIT_S),
                validator.getParameters()));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    unitTime =
        Util.parseTime(parameters.getStringOrDefault(TIME_UNIT_KEY, TIME_UNIT_S), parameters);
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

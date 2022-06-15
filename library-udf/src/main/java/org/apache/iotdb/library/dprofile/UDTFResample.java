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

import org.apache.iotdb.library.dprofile.util.Resampler;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.text.SimpleDateFormat;

/** This function does upsample or downsample of input series. */
public class UDTFResample implements UDTF {

  private Resampler resampler;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validate(
            x -> (long) x > 0,
            "gap should be a time period whose unit is ms, s, m, h, d.",
            Util.parseTime(validator.getParameters().getString("every")))
        .validate(
            x ->
                "min".equals(x)
                    || "max".equals(x)
                    || "mean".equals(x)
                    || "median".equals(x)
                    || "first".equals(x)
                    || "last".equals(x),
            "aggr should be min, max, mean, median, first, last.",
            validator.getParameters().getStringOrDefault("aggr", "mean").toLowerCase())
        .validate(
            x -> "nan".equals(x) || "ffill".equals(x) || "bfill".equals(x) || "linear".equals(x),
            "aggr should be min, max, mean, median, first, last.",
            validator.getParameters().getStringOrDefault("interp", "nan").toLowerCase());
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    if (validator.getParameters().hasAttribute("start")) {
      validator.validate(
          x -> (long) x > 0,
          "start should conform to the format yyyy-MM-dd HH:mm:ss.",
          format.parse(validator.getParameters().getString("start")).getTime());
    }
    if (validator.getParameters().hasAttribute("end")) {
      validator.validate(
          x -> (long) x > 0,
          "end should conform to the format yyyy-MM-dd HH:mm:ss.",
          format.parse(validator.getParameters().getString("end")).getTime());
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    long newPeriod = Util.parseTime(parameters.getString("every"));
    String aggregator = parameters.getStringOrDefault("aggr", "mean").toLowerCase();
    String interpolator = parameters.getStringOrDefault("interp", "nan").toLowerCase();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long startTime = -1, endTime = -1;
    if (parameters.hasAttribute("start")) {
      startTime = format.parse(parameters.getString("start")).getTime();
    }
    if (parameters.hasAttribute("end")) {
      endTime = format.parse(parameters.getString("end")).getTime();
    }
    resampler = new Resampler(newPeriod, aggregator, interpolator, startTime, endTime);
  }

  @Override
  public void transform(Row row, PointCollector pc) throws Exception {
    resampler.insert(row.getTime(), Util.getValueAsDouble(row));
    while (resampler.hasNext()) { // output as early as possible
      pc.putDouble(resampler.getOutTime(), resampler.getOutValue());
      resampler.next();
    }
  }

  @Override
  public void terminate(PointCollector pc) throws Exception {
    resampler.flush();
    while (resampler.hasNext()) {
      pc.putDouble(resampler.getOutTime(), resampler.getOutValue());
      resampler.next();
    }
  }
}

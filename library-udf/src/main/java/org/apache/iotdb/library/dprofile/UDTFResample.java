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

/** This function does upsample or downsample of input series. */
public class UDTFResample implements UDTF {
  private static final String START_PARAM = "start";
  private Resampler resampler;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {

    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validateRequiredAttribute("every")
        .validate(
            x -> Util.isPositiveTime((String) x, validator.getParameters()),
            "gap should be a time period whose unit is ms, s, m, h, d.",
            validator.getParameters().getString("every"))
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
    if (validator.getParameters().hasAttribute(START_PARAM)) {
      validator.validate(
          x -> Util.isPositiveDateTime((String) x),
          "start should conform to the format yyyy-MM-dd HH:mm:ss.",
          validator.getParameters().getString(START_PARAM));
    }
    if (validator.getParameters().hasAttribute("end")) {
      validator.validate(
          x -> Util.isPositiveDateTime((String) x),
          "end should conform to the format yyyy-MM-dd HH:mm:ss.",
          validator.getParameters().getString("end"));
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    long newPeriod = Util.parseTime(parameters.getString("every"), parameters);
    String aggregator = parameters.getStringOrDefault("aggr", "mean").toLowerCase();
    String interpolator = parameters.getStringOrDefault("interp", "nan").toLowerCase();
    long startTime = -1;
    long endTime = -1;
    if (parameters.hasAttribute(START_PARAM)) {
      startTime = Util.parseDateTime(parameters.getString(START_PARAM));
    }
    if (parameters.hasAttribute("end")) {
      endTime = Util.parseDateTime(parameters.getString("end"));
    }
    resampler = new Resampler(newPeriod, aggregator, interpolator, startTime, endTime);
  }

  @Override
  public void transform(Row row, PointCollector pc) throws Exception {
    if (!row.isNull(0)) {
      double v = Util.getValueAsDouble(row);
      if (Double.isFinite(v)) {
        resampler.insert(row.getTime(), v);
      }
    }
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

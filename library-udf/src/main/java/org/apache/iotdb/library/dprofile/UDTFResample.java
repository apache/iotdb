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
import org.apache.iotdb.library.i18n.LibraryUdfMessages;
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
  private static final String START_PARAM = "start";
  private Resampler resampler;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {

    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validate(
            x -> (long) x > 0,
            LibraryUdfMessages
                .EXCEPTION_GAP_SHOULD_BE_A_TIME_PERIOD_WHOSE_UNIT_IS_MS_S_M_H_D_0B4CA46A,
            Util.parseTime(validator.getParameters().getString("every"), validator.getParameters()))
        .validate(
            x ->
                "min".equals(x)
                    || "max".equals(x)
                    || "mean".equals(x)
                    || "median".equals(x)
                    || "first".equals(x)
                    || "last".equals(x),
            LibraryUdfMessages.EXCEPTION_AGGR_SHOULD_BE_MIN_MAX_MEAN_MEDIAN_FIRST_LAST_3588D6DE,
            validator.getParameters().getStringOrDefault("aggr", "mean").toLowerCase())
        .validate(
            x -> "nan".equals(x) || "ffill".equals(x) || "bfill".equals(x) || "linear".equals(x),
            LibraryUdfMessages.EXCEPTION_AGGR_SHOULD_BE_MIN_MAX_MEAN_MEDIAN_FIRST_LAST_3588D6DE,
            validator.getParameters().getStringOrDefault("interp", "nan").toLowerCase());
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    if (validator.getParameters().hasAttribute(START_PARAM)) {
      validator.validate(
          x -> (long) x > 0,
          LibraryUdfMessages
              .EXCEPTION_START_SHOULD_CONFORM_TO_THE_FORMAT_YYYY_MM_DD_HH_MM_SS_3FAAA2A2,
          format.parse(validator.getParameters().getString(START_PARAM)).getTime());
    }
    if (validator.getParameters().hasAttribute("end")) {
      validator.validate(
          x -> (long) x > 0,
          LibraryUdfMessages
              .EXCEPTION_END_SHOULD_CONFORM_TO_THE_FORMAT_YYYY_MM_DD_HH_MM_SS_F7135FE6,
          format.parse(validator.getParameters().getString("end")).getTime());
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    long newPeriod = Util.parseTime(parameters.getString("every"), parameters);
    String aggregator = parameters.getStringOrDefault("aggr", "mean").toLowerCase();
    String interpolator = parameters.getStringOrDefault("interp", "nan").toLowerCase();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long startTime = -1;
    long endTime = -1;
    if (parameters.hasAttribute(START_PARAM)) {
      startTime = format.parse(parameters.getString(START_PARAM)).getTime();
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

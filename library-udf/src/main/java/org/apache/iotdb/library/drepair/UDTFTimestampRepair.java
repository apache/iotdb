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

package org.apache.iotdb.library.drepair;

import org.apache.iotdb.library.drepair.util.TimestampRepair;
import org.apache.iotdb.library.i18n.LibraryUdfMessages;
import org.apache.iotdb.library.util.TypeServices;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

/** This function is used for timestamp repair. */
public class UDTFTimestampRepair implements UDTF {
  String intervalMethod;
  long interval;
  long intervalMode;
  private TypeServices.NumericWindowWriter windowWriter;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64);

    String intervalString = validator.getParameters().getStringOrDefault("interval", null);
    if (intervalString != null) {
      try {
        interval = Long.parseLong(intervalString);
      } catch (NumberFormatException e) {
        try {
          interval = Util.parseTime(intervalString, validator.getParameters());
        } catch (Exception ex) {
          throw new UDFException(LibraryUdfMessages.INVALID_TIME_FORMAT_FOR_INTERVAL);
        }
      }
      validator.validate(
          x -> interval > 0,
          "Invalid time unit input. Supported units are ns, us, ms, s, m, h, d.");
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE))
        .setOutputDataType(parameters.getDataType(0));
    windowWriter =
        TypeServices.NUMERIC_CAST_WINDOW_WRITER_SERVICE.call(
            TypeServices.toReadType(parameters.getDataType(0)));

    intervalMethod = parameters.getStringOrDefault("method", "Median");
    String intervalString = parameters.getStringOrDefault("interval", null);

    if (intervalString != null) {
      try {
        interval = Long.parseLong(intervalString);
      } catch (NumberFormatException e) {
        interval = Util.parseTime(intervalString, parameters);
      }
    } else {
      interval = 0;
    }

    if (interval > 0) {
      intervalMode = interval;
    } else if ("Median".equalsIgnoreCase(intervalMethod)) {
      intervalMode = -1L;
    } else if ("Mode".equalsIgnoreCase(intervalMethod)) {
      intervalMode = -2L;
    } else if ("Cluster".equalsIgnoreCase(intervalMethod)) {
      intervalMode = -3L;
    } else {
      throw new UDFException(LibraryUdfMessages.ILLEGAL_METHOD_WITH_DOT);
    }
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    TimestampRepair ts = new TimestampRepair(rowWindow.getRowIterator(), intervalMode, 2);
    ts.dpRepair();
    long[] timestamp = ts.getRepaired();
    double[] value = ts.getRepairedValue();
    windowWriter.write(timestamp, value, collector);
  }
}

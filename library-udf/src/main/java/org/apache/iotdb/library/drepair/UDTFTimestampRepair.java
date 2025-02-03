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

import org.apache.iotdb.library.drepair.util.TimesegmentRepair;
import org.apache.iotdb.library.drepair.util.TimestampRepair;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

public class UDTFTimestampRepair implements UDTF {
  String intervalMethod;
  Boolean segmentation;
  int interval;
  int intervalMode;
  int num_interval;
  long[] timestamp;
  double[] value;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validate(
            x -> (Integer) x >= 0,
            "Interval should be a positive integer.",
            validator.getParameters().getIntOrDefault("interval", 0));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE))
        .setOutputDataType(parameters.getDataType(0));
    segmentation = parameters.getBooleanOrDefault("isSegment", false);
    intervalMethod = parameters.getStringOrDefault("method", "Median");
    interval = parameters.getIntOrDefault("interval", 0);
    num_interval = parameters.getIntOrDefault("num_interval", 1);
    if (segmentation) {
      if (num_interval > 10) {
        throw new UDFException("The calculation interval is too large, causing slow execution!");
      } else if (num_interval <= 0) {
        throw new UDFException("Error num_interval!");
      }
    } else {
      if (interval > 0) {
        intervalMode = interval;
      } else if ("Median".equalsIgnoreCase(intervalMethod)) {
        intervalMode = -1;
      } else if ("Mode".equalsIgnoreCase(intervalMethod)) {
        intervalMode = -2;
      } else if ("Cluster".equalsIgnoreCase(intervalMethod)) {
        intervalMode = -3;
      } else {
        throw new UDFException("Illegal method.");
      }
    }
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (segmentation) {
      TimesegmentRepair tseg = new TimesegmentRepair(rowWindow.getRowIterator(), this.num_interval);
      tseg.exactRepair();
      timestamp = tseg.getRepaired();
      value = tseg.getRepairedValue();
    } else {
      TimestampRepair ts = new TimestampRepair(rowWindow.getRowIterator(), intervalMode, 2);
      ts.dpRepair();
      timestamp = ts.getRepaired();
      value = ts.getRepairedValue();
    }
    switch (rowWindow.getDataType(0)) {
      case DOUBLE:
        for (int i = 0; i < timestamp.length; i++) {
          collector.putDouble(timestamp[i], value[i]);
        }
        break;
      case FLOAT:
        for (int i = 0; i < timestamp.length; i++) {
          collector.putFloat(timestamp[i], (float) value[i]);
        }
        break;
      case INT32:
        for (int i = 0; i < timestamp.length; i++) {
          collector.putInt(timestamp[i], (int) value[i]);
        }
        break;
      case INT64:
        for (int i = 0; i < timestamp.length; i++) {
          collector.putLong(timestamp[i], (long) value[i]);
        }
        break;
      default:
        throw new UDFException("");
    }
  }
}

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

package org.apache.iotdb.library.anomaly;

import org.apache.iotdb.library.anomaly.util.WindowDetect;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

/**
 * The function is used to filter anomalies of a numeric time series based on two-sided window
 * detection.
 */
public class UDTFTwoSidedFilter implements UDTF {
  private double len;
  private double threshold;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE))
        .setOutputDataType(parameters.getDataType(0));
    this.len = parameters.getDoubleOrDefault("len", 5);
    this.threshold = parameters.getDoubleOrDefault("threshold", 0.4);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    WindowDetect wd = new WindowDetect(rowWindow.getRowIterator(), len, threshold);
    double[] repaired = wd.getRepaired();
    long[] time = wd.getTime();
    switch (rowWindow.getDataType(0)) {
      case DOUBLE:
        for (int i = 0; i < time.length; i++) {
          collector.putDouble(time[i], repaired[i]);
        }
        break;
      case FLOAT:
        for (int i = 0; i < time.length; i++) {
          collector.putFloat(time[i], (float) repaired[i]);
        }
        break;
      case INT32:
        for (int i = 0; i < time.length; i++) {
          collector.putInt(time[i], (int) Math.round(repaired[i]));
        }
        break;
      case INT64:
        for (int i = 0; i < time.length; i++) {
          collector.putLong(time[i], Math.round(repaired[i]));
        }
        break;
      case BOOLEAN:
      case BLOB:
      case STRING:
      case TEXT:
      case TIMESTAMP:
      case DATE:
      default:
        throw new UDFException("No such kind of data type.");
    }
  }

  @Override
  public void terminate(PointCollector collector)
      throws Exception { // default implementation ignored
  }
}

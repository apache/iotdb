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

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.library.util.DoubleCircularQueue;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/** This function calculates moving average of given window length of input series. */
public class UDTFMvAvg implements UDTF {
  int windowSize;
  TSDataType dataType;
  DoubleCircularQueue v;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            x -> (int) x > 0,
            "Window size should be larger than 0.",
            validator.getParameters().getIntOrDefault("window", 10));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
    windowSize = parameters.getIntOrDefault("window", 10);
    v = new DoubleCircularQueue(windowSize);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    long t = row.getTime();
    double windowSum = 0d;
    if (v.isFull()) {
      windowSum -= v.pop();
    }
    double value = Util.getValueAsDouble(row);
    if (Double.isFinite(value)) {
      v.push(value);
      windowSum += value;
      if (v.isFull()) {
        collector.putDouble(t, windowSum / (double) windowSize);
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {}
}

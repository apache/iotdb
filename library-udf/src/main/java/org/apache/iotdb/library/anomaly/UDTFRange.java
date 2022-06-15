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

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
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

/** This function is used to detect range anomaly of time series. */
public class UDTFRange implements UDTF {
  private TSDataType dataType;
  private double upperBound;
  private double lowerBound;

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
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(parameters.getDataType(0));
    this.lowerBound = parameters.getDouble("lower_bound");
    this.upperBound = parameters.getDouble("upper_bound");
    this.dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    int intValue;
    long longValue;
    float floatValue;
    double doubleValue;
    long timestamp;
    timestamp = row.getTime();
    switch (dataType) {
      case INT32:
        intValue = row.getInt(0);
        if (intValue > upperBound || intValue < lowerBound) {
          Util.putValue(collector, dataType, timestamp, intValue);
        }
        break;
      case INT64:
        longValue = row.getLong(0);
        if (longValue > upperBound || longValue < lowerBound) {
          Util.putValue(collector, dataType, timestamp, longValue);
        }
        break;
      case FLOAT:
        floatValue = row.getFloat(0);
        if (floatValue > upperBound || floatValue < lowerBound) {
          Util.putValue(collector, dataType, timestamp, floatValue);
        }
        break;
      case DOUBLE:
        doubleValue = row.getDouble(0);
        if (doubleValue > upperBound || doubleValue < lowerBound) {
          Util.putValue(collector, dataType, timestamp, doubleValue);
        }
        break;
      default:
        throw new Exception("No such kind of data type.");
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {}
}

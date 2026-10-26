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

import org.apache.iotdb.library.util.TypeServices;
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
  private double upperBound;
  private double lowerBound;
  private TypeServices.NumericRowReader rowReader;
  private TypeServices.NumericRowWriter rowWriter;

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
    Type dataType = parameters.getDataType(0);
    org.apache.tsfile.read.common.type.Type type = TypeServices.toReadType(dataType);
    rowReader = TypeServices.NUMERIC_ROW_READER_SERVICE.call(type);
    rowWriter = TypeServices.NUMERIC_ROW_WRITER_SERVICE.call(type);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double value = rowReader.read(row);
    if (value > upperBound || value < lowerBound) {
      rowWriter.write(row, collector);
    }
  }

  @Override
  public void terminate(PointCollector collector)
      throws Exception { // default implementation ignored
  }
}

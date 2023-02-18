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
import org.apache.iotdb.library.util.NoNumberException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/**
 * This function is used to calculate the spread of time series, that is, the maximum value minus
 * the minimum value.
 */
public class UDAFSpread implements UDTF {

  int intMin = Integer.MAX_VALUE, intMax = Integer.MIN_VALUE;
  long longMin = Long.MAX_VALUE, longMax = Long.MIN_VALUE;
  float floatMin = Float.MAX_VALUE, floatMax = -Float.MAX_VALUE;
  double doubleMin = Double.MAX_VALUE, doubleMax = -Double.MAX_VALUE;
  TSDataType dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(dataType));
  }

  @Override
  public void transform(Row row, PointCollector pc) throws Exception {
    switch (dataType) {
      case INT32:
        transformInt(row, pc);
        break;
      case INT64:
        transformLong(row, pc);
        break;
      case FLOAT:
        transformFloat(row, pc);
        break;
      case DOUBLE:
        transformDouble(row, pc);
        break;
    }
  }

  @Override
  public void terminate(PointCollector pc) throws Exception {
    switch (dataType) {
      case INT32:
        pc.putInt(0, intMax - intMin);
        break;
      case INT64:
        pc.putLong(0, longMax - longMin);
        break;
      case FLOAT:
        pc.putFloat(0, floatMax - floatMin);
        break;
      case DOUBLE:
        pc.putDouble(0, doubleMax - doubleMin);
        break;
      default:
        throw new NoNumberException();
    }
  }

  private void transformInt(Row row, PointCollector pc) throws Exception {
    int v = row.getInt(0);
    intMin = Math.min(intMin, v);
    intMax = Math.max(intMax, v);
  }

  private void transformLong(Row row, PointCollector pc) throws Exception {
    long v = row.getLong(0);
    longMin = Math.min(longMin, v);
    longMax = Math.max(longMax, v);
  }

  private void transformFloat(Row row, PointCollector pc) throws Exception {
    float v = row.getFloat(0);
    if (Float.isFinite(v)) {
      floatMin = Math.min(floatMin, v);
      floatMax = Math.max(floatMax, v);
    }
  }

  private void transformDouble(Row row, PointCollector pc) throws Exception {
    double v = row.getDouble(0);
    if (Double.isFinite(v)) {
      doubleMin = doubleMin < v ? doubleMin : v;
      doubleMax = doubleMax > v ? doubleMax : v;
    }
  }
}

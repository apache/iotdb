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

import org.apache.iotdb.library.util.NoNumberException;
import org.apache.iotdb.library.util.TypeServices;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

/**
 * This function is used to calculate the spread of time series, that is, the maximum value minus
 * the minimum value.
 */
public class UDAFSpread implements UDTF {

  int intMin = Integer.MAX_VALUE;
  int intMax = Integer.MIN_VALUE;
  long longMin = Long.MAX_VALUE;
  long longMax = Long.MIN_VALUE;
  float floatMin = Float.MAX_VALUE;
  float floatMax = -Float.MAX_VALUE;
  double doubleMin = Double.MAX_VALUE;
  double doubleMax = -Double.MAX_VALUE;
  private SpreadTransformer transformer;
  private SpreadTerminator terminator;

  private static final SpreadTransformer INT_TRANSFORMER = UDAFSpread::transformInt;
  private static final SpreadTransformer LONG_TRANSFORMER = UDAFSpread::transformLong;
  private static final SpreadTransformer FLOAT_TRANSFORMER = UDAFSpread::transformFloat;
  private static final SpreadTransformer DOUBLE_TRANSFORMER = UDAFSpread::transformDouble;
  private static final SpreadTransformer UNSUPPORTED_TRANSFORMER = (target, row) -> {};

  private static final SpreadTerminator INT_TERMINATOR =
      (target, collector) -> collector.putInt(0, target.intMax - target.intMin);
  private static final SpreadTerminator LONG_TERMINATOR =
      (target, collector) -> collector.putLong(0, target.longMax - target.longMin);
  private static final SpreadTerminator FLOAT_TERMINATOR =
      (target, collector) -> collector.putFloat(0, target.floatMax - target.floatMin);
  private static final SpreadTerminator DOUBLE_TERMINATOR =
      (target, collector) -> collector.putDouble(0, target.doubleMax - target.doubleMin);
  private static final SpreadTerminator UNSUPPORTED_TERMINATOR =
      (target, collector) -> {
        throw new NoNumberException();
      };

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    Type dataType = parameters.getDataType(0);
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(dataType);
    org.apache.tsfile.read.common.type.Type type = TypeServices.toReadType(dataType);
    transformer =
        TypeServices.numericService(
                INT_TRANSFORMER,
                LONG_TRANSFORMER,
                FLOAT_TRANSFORMER,
                DOUBLE_TRANSFORMER,
                UNSUPPORTED_TRANSFORMER)
            .call(type);
    terminator =
        TypeServices.numericService(
                INT_TERMINATOR,
                LONG_TERMINATOR,
                FLOAT_TERMINATOR,
                DOUBLE_TERMINATOR,
                UNSUPPORTED_TERMINATOR)
            .call(type);
  }

  @Override
  public void transform(Row row, PointCollector pc) throws Exception {
    transformer.transform(this, row);
  }

  @Override
  public void terminate(PointCollector pc) throws Exception {
    terminator.terminate(this, pc);
  }

  private void transformInt(Row row) throws IOException {
    int v = row.getInt(0);
    intMin = Math.min(intMin, v);
    intMax = Math.max(intMax, v);
  }

  private void transformLong(Row row) throws IOException {
    long v = row.getLong(0);
    longMin = Math.min(longMin, v);
    longMax = Math.max(longMax, v);
  }

  private void transformFloat(Row row) throws IOException {
    float v = row.getFloat(0);
    if (Float.isFinite(v)) {
      floatMin = Math.min(floatMin, v);
      floatMax = Math.max(floatMax, v);
    }
  }

  private void transformDouble(Row row) throws IOException {
    double v = row.getDouble(0);
    if (Double.isFinite(v)) {
      doubleMin = Math.min(doubleMin, v);
      doubleMax = Math.max(doubleMax, v);
    }
  }

  @FunctionalInterface
  private interface SpreadTransformer {
    void transform(UDAFSpread target, Row row) throws IOException;
  }

  @FunctionalInterface
  private interface SpreadTerminator {
    void terminate(UDAFSpread target, PointCollector collector)
        throws IOException, NoNumberException;
  }
}

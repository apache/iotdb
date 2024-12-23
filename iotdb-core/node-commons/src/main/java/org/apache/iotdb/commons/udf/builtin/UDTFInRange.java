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

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;

import java.io.IOException;

public class UDTFInRange implements UDTF {
  protected TSDataType dataType;
  protected double upper;
  protected double lower;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validateRequiredAttribute("upper")
        .validateRequiredAttribute("lower")
        .validate(
            args -> (Double) args[0] >= (Double) args[1],
            "Upper can not be smaller than lower.",
            validator.getParameters().getDouble("upper"),
            validator.getParameters().getDouble("lower"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException {
    upper = parameters.getDouble("upper");
    lower = parameters.getDouble("lower");
    dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
    configurations
        .setAccessStrategy(new MappableRowByRowAccessStrategy())
        .setOutputDataType(Type.BOOLEAN);
  }

  @Override
  public void transform(Row row, PointCollector collector)
      throws UDFInputSeriesDataTypeNotValidException, IOException {
    long time = row.getTime();
    switch (dataType) {
      case INT32:
        collector.putBoolean(time, row.getInt(0) >= lower && upper >= row.getInt(0));
        break;
      case INT64:
        collector.putBoolean(time, row.getLong(0) >= lower && upper >= row.getLong(0));
        break;
      case FLOAT:
        collector.putBoolean(time, row.getFloat(0) >= lower && upper >= row.getFloat(0));
        break;
      case DOUBLE:
        collector.putBoolean(time, row.getDouble(0) >= lower && upper >= row.getDouble(0));
        break;
      case BLOB:
      case TEXT:
      case DATE:
      case STRING:
      case TIMESTAMP:
      case BOOLEAN:
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
    }
  }

  @Override
  public Object transform(Row row) throws Exception {
    if (row.isNull(0)) {
      return null;
    }
    switch (dataType) {
      case INT32:
        return row.getInt(0) >= lower && upper >= row.getInt(0);
      case INT64:
        return row.getLong(0) >= lower && upper >= row.getLong(0);
      case FLOAT:
        return row.getFloat(0) >= lower && upper >= row.getFloat(0);
      case DOUBLE:
        return row.getDouble(0) >= lower && upper >= row.getDouble(0);
      case TIMESTAMP:
      case BOOLEAN:
      case DATE:
      case STRING:
      case TEXT:
      case BLOB:
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
    }
  }

  @Override
  public void transform(Column[] columns, ColumnBuilder builder) throws Exception {
    switch (dataType) {
      case INT32:
        transformInt(columns, builder);
        return;
      case INT64:
        transformLong(columns, builder);
        return;
      case FLOAT:
        transformFloat(columns, builder);
        return;
      case DOUBLE:
        transformDouble(columns, builder);
        return;
      case BLOB:
      case TEXT:
      case DATE:
      case STRING:
      case BOOLEAN:
      case TIMESTAMP:
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
    }
  }

  private void transformInt(Column[] columns, ColumnBuilder builder) {
    int[] inputs = columns[0].getInts();
    boolean[] isNulls = columns[0].isNull();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (isNulls[i]) {
        builder.appendNull();
      } else {
        boolean res = inputs[i] >= lower && upper >= inputs[i];
        builder.writeBoolean(res);
      }
    }
  }

  private void transformLong(Column[] columns, ColumnBuilder builder) {
    long[] inputs = columns[0].getLongs();
    boolean[] isNulls = columns[0].isNull();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (isNulls[i]) {
        builder.appendNull();
      } else {
        boolean res = inputs[i] >= lower && upper >= inputs[i];
        builder.writeBoolean(res);
      }
    }
  }

  private void transformFloat(Column[] columns, ColumnBuilder builder) {
    float[] inputs = columns[0].getFloats();
    boolean[] isNulls = columns[0].isNull();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (isNulls[i]) {
        builder.appendNull();
      } else {
        boolean res = inputs[i] >= lower && upper >= inputs[i];
        builder.writeBoolean(res);
      }
    }
  }

  private void transformDouble(Column[] columns, ColumnBuilder builder) {
    double[] inputs = columns[0].getDoubles();
    boolean[] isNulls = columns[0].isNull();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (isNulls[i]) {
        builder.appendNull();
      } else {
        boolean res = inputs[i] >= lower && upper >= inputs[i];
        builder.writeBoolean(res);
      }
    }
  }
}

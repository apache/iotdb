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

public abstract class UDTFMath implements UDTF {

  protected interface Transformer {

    double transform(double operand);
  }

  protected Transformer transformer;

  protected TSDataType dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException {
    dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
    configurations
        .setAccessStrategy(new MappableRowByRowAccessStrategy())
        .setOutputDataType(Type.DOUBLE);
    setTransformer();
  }

  protected abstract void setTransformer();

  @Override
  public void transform(Row row, PointCollector collector)
      throws UDFInputSeriesDataTypeNotValidException, IOException {
    long time = row.getTime();
    switch (dataType) {
      case INT32:
        collector.putDouble(time, transformer.transform(row.getInt(0)));
        break;
      case INT64:
        collector.putDouble(time, transformer.transform(row.getLong(0)));
        break;
      case FLOAT:
        collector.putDouble(time, transformer.transform(row.getFloat(0)));
        break;
      case DOUBLE:
        collector.putDouble(time, transformer.transform(row.getDouble(0)));
        break;
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
  public Object transform(Row row) throws IOException {
    if (row.isNull(0)) {
      return null;
    }
    switch (dataType) {
      case INT32:
        return transformer.transform(row.getInt(0));
      case INT64:
        return transformer.transform(row.getLong(0));
      case FLOAT:
        return transformer.transform(row.getFloat(0));
      case DOUBLE:
        return transformer.transform(row.getDouble(0));
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
        builder.writeDouble(transformer.transform(inputs[i]));
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
        builder.writeDouble(transformer.transform(inputs[i]));
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
        builder.writeDouble(transformer.transform(inputs[i]));
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
        builder.writeDouble(transformer.transform(inputs[i]));
      }
    }
  }
}

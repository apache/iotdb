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
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.io.IOException;

@SuppressWarnings("java:S2177")
public class UDTFAbs extends UDTFMath {

  private TypeServices.AbsRowCollector rowCollector;
  private TypeServices.AbsRowMapper rowMapper;
  private TypeServices.AbsColumnTransformer columnTransformer;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException {
    dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
    Type type = Type.fromTsDataType(dataType);
    rowCollector = TypeServices.ABS_ROW_COLLECTOR_SERVICE.call(type);
    rowMapper = TypeServices.ABS_ROW_MAPPER_SERVICE.call(type);
    columnTransformer = TypeServices.ABS_COLUMN_TRANSFORMER_SERVICE.call(type);
    configurations
        .setAccessStrategy(new MappableRowByRowAccessStrategy())
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(dataType));
  }

  @Override
  public void transform(Row row, PointCollector collector) throws IOException {
    rowCollector.collect(row, collector);
  }

  @Override
  public Object transform(Row row) throws IOException {
    if (row.isNull(0)) {
      return null;
    }
    return rowMapper.map(row);
  }

  @Override
  public void transform(Column[] columns, ColumnBuilder builder) throws Exception {
    columnTransformer.transform(this, columns, builder);
  }

  @Override
  protected void setTransformer() {
    throw new UnsupportedOperationException(CommonMessages.UDTF_ABS_SET_TRANSFORMER);
  }

  protected void transformInt(Column[] columns, ColumnBuilder builder) {
    int[] inputs = columns[0].getInts();
    boolean[] isNulls = columns[0].isNull();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (isNulls[i]) {
        builder.appendNull();
      } else {
        builder.writeInt(Math.abs(inputs[i]));
      }
    }
  }

  protected void transformLong(Column[] columns, ColumnBuilder builder) {
    long[] inputs = columns[0].getLongs();
    boolean[] isNulls = columns[0].isNull();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (isNulls[i]) {
        builder.appendNull();
      } else {
        builder.writeLong(Math.abs(inputs[i]));
      }
    }
  }

  protected void transformFloat(Column[] columns, ColumnBuilder builder) {
    float[] inputs = columns[0].getFloats();
    boolean[] isNulls = columns[0].isNull();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (isNulls[i]) {
        builder.appendNull();
      } else {
        builder.writeFloat(Math.abs(inputs[i]));
      }
    }
  }

  protected void transformDouble(Column[] columns, ColumnBuilder builder) {
    double[] inputs = columns[0].getDoubles();
    boolean[] isNulls = columns[0].isNull();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (isNulls[i]) {
        builder.appendNull();
      } else {
        builder.writeDouble(Math.abs(inputs[i]));
      }
    }
  }
}

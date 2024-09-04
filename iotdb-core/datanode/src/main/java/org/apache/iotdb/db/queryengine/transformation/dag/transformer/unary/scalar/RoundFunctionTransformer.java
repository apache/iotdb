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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.scalar;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.UnaryTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;

import java.io.IOException;

public class RoundFunctionTransformer extends UnaryTransformer {
  private final TSDataType targetDataType;

  protected int places;

  public RoundFunctionTransformer(LayerReader layerReader, TSDataType targetDataType, int places) {
    super(layerReader);
    this.targetDataType = targetDataType;
    this.places = places;
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {targetDataType};
  }

  @Override
  protected void transform(Column[] columns, ColumnBuilder builder)
      throws QueryProcessException, IOException {
    switch (layerReaderDataType) {
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
      case TIMESTAMP:
      case BOOLEAN:
      case DATE:
      case STRING:
      case TEXT:
      case BLOB:
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported source dataType: %s", layerReaderDataType));
    }
  }

  private void transformInt(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();

    int[] values = columns[0].getInts();
    boolean[] isNulls = columns[0].isNull();
    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        double res = Math.rint(values[i] * Math.pow(10, places)) / Math.pow(10, places);
        builder.writeDouble(res);
      } else {
        builder.appendNull();
      }
    }
  }

  private void transformLong(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();

    long[] values = columns[0].getLongs();
    boolean[] isNulls = columns[0].isNull();
    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        double res = Math.rint(values[i] * Math.pow(10, places)) / Math.pow(10, places);
        builder.writeDouble(res);
      } else {
        builder.appendNull();
      }
    }
  }

  private void transformFloat(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();

    float[] values = columns[0].getFloats();
    boolean[] isNulls = columns[0].isNull();
    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        double res = Math.rint(values[i] * Math.pow(10, places)) / Math.pow(10, places);
        builder.writeDouble(res);
      } else {
        builder.appendNull();
      }
    }
  }

  private void transformDouble(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();

    double[] values = columns[0].getDoubles();
    boolean[] isNulls = columns[0].isNull();
    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        double res = Math.rint(values[i] * Math.pow(10, places)) / Math.pow(10, places);
        builder.writeDouble(res);
      } else {
        builder.appendNull();
      }
    }
  }
}

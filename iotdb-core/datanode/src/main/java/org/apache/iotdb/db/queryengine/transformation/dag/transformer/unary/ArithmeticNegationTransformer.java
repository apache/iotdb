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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;

import java.io.IOException;

public class ArithmeticNegationTransformer extends UnaryTransformer {

  public ArithmeticNegationTransformer(LayerReader layerReader) {
    super(layerReader);
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {layerReaderDataType};
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
      case DATE:
      case TEXT:
      case TIMESTAMP:
      case BLOB:
      case BOOLEAN:
      case STRING:
      default:
        throw new QueryProcessException("Unsupported data type: " + layerReaderDataType);
    }
  }

  private void transformInt(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();
    int[] values = columns[0].getInts();
    boolean[] isNulls = columns[0].isNull();

    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        builder.writeInt(-values[i]);
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
        builder.writeLong(-values[i]);
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
        builder.writeFloat(-values[i]);
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
        builder.writeDouble(-values[i]);
      } else {
        builder.appendNull();
      }
    }
  }
}

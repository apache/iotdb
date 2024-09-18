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
import org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.helper.CastFunctionHelper;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.UnaryTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;

import java.io.IOException;

public class CastFunctionTransformer extends UnaryTransformer {
  private final TSDataType targetDataType;

  public CastFunctionTransformer(LayerReader layerReader, TSDataType targetDataType) {
    super(layerReader);
    this.targetDataType = targetDataType;
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {targetDataType};
  }

  @Override
  protected Column[] transform(Column[] columns) throws QueryProcessException, IOException {
    switch (layerReaderDataType) {
      case INT32:
        return castInts(columns);
      case INT64:
        return castLongs(columns);
      case FLOAT:
        return castFloats(columns);
      case DOUBLE:
        return castDoubles(columns);
      case BOOLEAN:
        return castBooleans(columns);
      case TEXT:
        return castBinaries(columns);
      case BLOB:
      case STRING:
      case TIMESTAMP:
      case DATE:
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported source dataType: %s", layerReaderDataType));
    }
  }

  private Column[] castInts(Column[] columns) {
    if (targetDataType == TSDataType.INT32) {
      return columns;
    }

    int count = columns[0].getPositionCount();
    int[] values = columns[0].getInts();
    boolean[] isNulls = columns[0].isNull();
    ColumnBuilder builder;
    switch (targetDataType) {
      case INT64:
        builder = new LongColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeLong(values[i]);
          } else {
            builder.appendNull();
          }
        }
        break;
      case FLOAT:
        builder = new FloatColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeFloat(values[i]);
          } else {
            builder.appendNull();
          }
        }
        break;
      case DOUBLE:
        builder = new DoubleColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeDouble(values[i]);
          } else {
            builder.appendNull();
          }
        }
        break;
      case BOOLEAN:
        builder = new BooleanColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeBoolean(values[i] != 0);
          } else {
            builder.appendNull();
          }
        }
        break;
      case TEXT:
        builder = new BinaryColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeBinary(BytesUtils.valueOf(String.valueOf(values[i])));
          } else {
            builder.appendNull();
          }
        }
        break;
      case STRING:
      case BLOB:
      case TIMESTAMP:
      case DATE:
      case INT32:
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerReaderDataType));
    }

    Column valueColumn = builder.build();
    Column timeColumn = columns[1];
    return new Column[] {valueColumn, timeColumn};
  }

  private Column[] castLongs(Column[] columns) {
    if (targetDataType == TSDataType.INT64) {
      return columns;
    }

    int count = columns[0].getPositionCount();
    long[] values = columns[0].getLongs();
    boolean[] isNulls = columns[0].isNull();
    ColumnBuilder builder;
    switch (targetDataType) {
      case INT32:
        builder = new IntColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeInt(CastFunctionHelper.castLongToInt(values[i]));
          } else {
            builder.appendNull();
          }
        }
        break;
      case FLOAT:
        builder = new FloatColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeFloat(values[i]);
          } else {
            builder.appendNull();
          }
        }
        break;
      case DOUBLE:
        builder = new DoubleColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeDouble(values[i]);
          } else {
            builder.appendNull();
          }
        }
        break;
      case BOOLEAN:
        builder = new BooleanColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeBoolean(values[i] != 0L);
          } else {
            builder.appendNull();
          }
        }
        break;
      case TEXT:
        builder = new BinaryColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeBinary(BytesUtils.valueOf(String.valueOf(values[i])));
          } else {
            builder.appendNull();
          }
        }
        break;
      case BLOB:
      case STRING:
      case DATE:
      case TIMESTAMP:
      case INT64:
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerReaderDataType));
    }

    Column valueColumn = builder.build();
    Column timeColumn = columns[1];
    return new Column[] {valueColumn, timeColumn};
  }

  private Column[] castFloats(Column[] columns) {
    if (targetDataType == TSDataType.FLOAT) {
      return columns;
    }

    int count = columns[0].getPositionCount();
    float[] values = columns[0].getFloats();
    boolean[] isNulls = columns[0].isNull();
    ColumnBuilder builder;
    switch (targetDataType) {
      case INT32:
        builder = new IntColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeInt(CastFunctionHelper.castFloatToInt(values[i]));
          } else {
            builder.appendNull();
          }
        }
        break;
      case INT64:
        builder = new LongColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeLong(CastFunctionHelper.castFloatToLong(values[i]));
          } else {
            builder.appendNull();
          }
        }
        break;
      case DOUBLE:
        builder = new DoubleColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeDouble(values[i]);
          } else {
            builder.appendNull();
          }
        }
        break;
      case BOOLEAN:
        builder = new BooleanColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeBoolean(values[i] != 0f);
          } else {
            builder.appendNull();
          }
        }
        break;
      case TEXT:
        builder = new BinaryColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeBinary(BytesUtils.valueOf(String.valueOf(values[i])));
          } else {
            builder.appendNull();
          }
        }
        break;
      case BLOB:
      case STRING:
      case TIMESTAMP:
      case DATE:
      case FLOAT:
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerReaderDataType));
    }

    Column valueColumn = builder.build();
    Column timeColumn = columns[1];
    return new Column[] {valueColumn, timeColumn};
  }

  private Column[] castDoubles(Column[] columns) {
    if (targetDataType == TSDataType.DOUBLE) {
      return columns;
    }

    int count = columns[0].getPositionCount();
    double[] values = columns[0].getDoubles();
    boolean[] isNulls = columns[0].isNull();
    ColumnBuilder builder;
    switch (targetDataType) {
      case INT32:
        builder = new IntColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeInt(CastFunctionHelper.castDoubleToInt(values[i]));
          } else {
            builder.appendNull();
          }
        }
        break;
      case INT64:
        builder = new LongColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeLong(CastFunctionHelper.castDoubleToLong(values[i]));
          } else {
            builder.appendNull();
          }
        }
        break;
      case FLOAT:
        builder = new FloatColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeFloat(CastFunctionHelper.castDoubleToFloat(values[i]));
          } else {
            builder.appendNull();
          }
        }
        break;
      case BOOLEAN:
        builder = new BooleanColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeBoolean(values[i] != 0.0);
          } else {
            builder.appendNull();
          }
        }
        break;
      case TEXT:
        builder = new BinaryColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeBinary(BytesUtils.valueOf(String.valueOf(values[i])));
          } else {
            builder.appendNull();
          }
        }
        break;
      case BLOB:
      case STRING:
      case TIMESTAMP:
      case DATE:
      case DOUBLE:
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerReaderDataType));
    }

    Column valueColumn = builder.build();
    Column timeColumn = columns[1];
    return new Column[] {valueColumn, timeColumn};
  }

  private Column[] castBooleans(Column[] columns) {
    if (targetDataType == TSDataType.BOOLEAN) {
      return columns;
    }

    int count = columns[0].getPositionCount();
    boolean[] values = columns[0].getBooleans();
    boolean[] isNulls = columns[0].isNull();
    ColumnBuilder builder;
    switch (targetDataType) {
      case INT32:
        builder = new IntColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeInt(values[i] ? 1 : 0);
          } else {
            builder.appendNull();
          }
        }
        break;
      case INT64:
        builder = new LongColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeLong(values[i] ? 1L : 0);
          } else {
            builder.appendNull();
          }
        }
        break;
      case FLOAT:
        builder = new FloatColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeFloat(values[i] ? 1.0f : 0);
          } else {
            builder.appendNull();
          }
        }
        break;
      case DOUBLE:
        builder = new DoubleColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeDouble(values[i] ? 1.0 : 0);
          } else {
            builder.appendNull();
          }
        }
        break;
      case TEXT:
        builder = new BinaryColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            builder.writeBinary(BytesUtils.valueOf(String.valueOf(values[i])));
          } else {
            builder.appendNull();
          }
        }
        break;
      case STRING:
      case BLOB:
      case DATE:
      case TIMESTAMP:
      case BOOLEAN:
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerReaderDataType));
    }

    Column valueColumn = builder.build();
    Column timeColumn = columns[1];
    return new Column[] {valueColumn, timeColumn};
  }

  private Column[] castBinaries(Column[] columns) {
    if (targetDataType == TSDataType.TEXT) {
      return columns;
    }

    int count = columns[0].getPositionCount();
    Binary[] values = columns[0].getBinaries();
    boolean[] isNulls = columns[0].isNull();
    ColumnBuilder builder;
    switch (targetDataType) {
      case INT32:
        builder = new IntColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            String str = values[i].getStringValue(TSFileConfig.STRING_CHARSET);
            builder.writeInt(Integer.parseInt(str));
          } else {
            builder.appendNull();
          }
        }
        break;
      case INT64:
        builder = new LongColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            String str = values[i].getStringValue(TSFileConfig.STRING_CHARSET);
            builder.writeLong(Long.parseLong(str));
          } else {
            builder.appendNull();
          }
        }
        break;
      case FLOAT:
        builder = new FloatColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            String str = values[i].getStringValue(TSFileConfig.STRING_CHARSET);
            builder.writeFloat(CastFunctionHelper.castTextToFloat(str));
          } else {
            builder.appendNull();
          }
        }
        break;
      case DOUBLE:
        builder = new DoubleColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            String str = values[i].getStringValue(TSFileConfig.STRING_CHARSET);
            builder.writeDouble(CastFunctionHelper.castTextToDouble(str));
          } else {
            builder.appendNull();
          }
        }
        break;
      case BOOLEAN:
        builder = new BooleanColumnBuilder(null, count);
        for (int i = 0; i < count; i++) {
          if (!isNulls[i]) {
            String str = values[i].getStringValue(TSFileConfig.STRING_CHARSET);
            builder.writeBoolean(CastFunctionHelper.castTextToBoolean(str));
          } else {
            builder.appendNull();
          }
        }
        break;
      case TIMESTAMP:
      case DATE:
      case STRING:
      case BLOB:
      case TEXT:
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerReaderDataType));
    }

    Column valueColumn = builder.build();
    Column timeColumn = columns[1];
    return new Column[] {valueColumn, timeColumn};
  }
}

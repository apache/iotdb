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

package org.apache.iotdb.db.mpp.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.utils.Binary;

public class CastFunctionColumnTransformer extends UnaryColumnTransformer {

  public CastFunctionColumnTransformer(Type returnType, ColumnTransformer childColumnTransformer) {
    super(returnType, childColumnTransformer);
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        switch (childColumnTransformer.getType().getTypeEnum()) {
          case INT32:
            cast(columnBuilder, childColumnTransformer.getType().getInt(column, i));
          case INT64:
            cast(columnBuilder, childColumnTransformer.getType().getLong(column, i));
          case FLOAT:
            cast(columnBuilder, childColumnTransformer.getType().getFloat(column, i));
          case DOUBLE:
            cast(columnBuilder, childColumnTransformer.getType().getDouble(column, i));
          case BOOLEAN:
            cast(columnBuilder, childColumnTransformer.getType().getBoolean(column, i));
          case BINARY:
            cast(columnBuilder, childColumnTransformer.getType().getBoolean(column, i));
        }
        returnType.writeDouble(
            columnBuilder, -childColumnTransformer.getType().getDouble(column, i));
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  private void cast(ColumnBuilder columnBuilder, int value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, value);
      case INT64:
        returnType.writeLong(columnBuilder, value);
      case FLOAT:
        returnType.writeFloat(columnBuilder, value);
      case DOUBLE:
        returnType.writeDouble(columnBuilder, value);
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, value != 0);
      case BINARY:
        returnType.writeBinary(columnBuilder, Binary.valueOf(String.valueOf(value)));
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }

  private void cast(ColumnBuilder columnBuilder, long value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, (int) value);
      case INT64:
        returnType.writeLong(columnBuilder, value);
      case FLOAT:
        returnType.writeFloat(columnBuilder, value);
      case DOUBLE:
        returnType.writeDouble(columnBuilder, value);
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, value != 0L);
      case BINARY:
        returnType.writeBinary(columnBuilder, Binary.valueOf(String.valueOf(value)));
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }

  private void cast(ColumnBuilder columnBuilder, float value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, (int) value);
      case INT64:
        returnType.writeLong(columnBuilder, (long) value);
      case FLOAT:
        returnType.writeFloat(columnBuilder, value);
      case DOUBLE:
        returnType.writeDouble(columnBuilder, value);
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, value != 0.0f);
      case BINARY:
        returnType.writeBinary(columnBuilder, Binary.valueOf(String.valueOf(value)));
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }

  private void cast(ColumnBuilder columnBuilder, double value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, (int) value);
      case INT64:
        returnType.writeLong(columnBuilder, (long) value);
      case FLOAT:
        returnType.writeFloat(columnBuilder, (float) value);
      case DOUBLE:
        returnType.writeDouble(columnBuilder, value);
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, value != 0.0);
      case BINARY:
        returnType.writeBinary(columnBuilder, Binary.valueOf(String.valueOf(value)));
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }

  private void cast(ColumnBuilder columnBuilder, boolean value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, value ? 1 : 0);
      case INT64:
        returnType.writeLong(columnBuilder, value ? 1L : 0);
      case FLOAT:
        returnType.writeFloat(columnBuilder, value ? 1.0f : 0);
      case DOUBLE:
        returnType.writeDouble(columnBuilder, value ? 1.0 : 0);
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, value);
      case BINARY:
        returnType.writeBinary(columnBuilder, Binary.valueOf(String.valueOf(value)));
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }

  private void cast(ColumnBuilder columnBuilder, Binary value) {
    String stringValue = value.getStringValue();
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, (int) Double.parseDouble(stringValue));
      case INT64:
        returnType.writeLong(columnBuilder, (long) Double.parseDouble(stringValue));
      case FLOAT:
        returnType.writeFloat(columnBuilder, (float) Double.parseDouble(stringValue));
      case DOUBLE:
        returnType.writeDouble(columnBuilder, Double.parseDouble(stringValue));
      case BOOLEAN:
        returnType.writeBoolean(
            columnBuilder, !("false".equals(stringValue) || "".equals(stringValue)));
      case BINARY:
        returnType.writeBinary(columnBuilder, value);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }
}

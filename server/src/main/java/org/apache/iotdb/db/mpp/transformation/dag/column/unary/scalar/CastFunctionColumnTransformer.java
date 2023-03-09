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

import org.apache.iotdb.db.mpp.plan.expression.multi.builtin.helper.CastHelper;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;
import org.apache.iotdb.tsfile.utils.Binary;

public class CastFunctionColumnTransformer extends UnaryColumnTransformer {

  public CastFunctionColumnTransformer(Type returnType, ColumnTransformer childColumnTransformer) {
    super(returnType, childColumnTransformer);
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    TypeEnum sourceType = childColumnTransformer.getType().getTypeEnum();
    Type childType = childColumnTransformer.getType();
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        switch (sourceType) {
          case INT32:
            cast(columnBuilder, childType.getInt(column, i));
            break;
          case INT64:
            cast(columnBuilder, childType.getLong(column, i));
            break;
          case FLOAT:
            cast(columnBuilder, childType.getFloat(column, i));
            break;
          case DOUBLE:
            cast(columnBuilder, childType.getDouble(column, i));
            break;
          case BOOLEAN:
            cast(columnBuilder, childType.getBoolean(column, i));
            break;
          case BINARY:
            cast(columnBuilder, childType.getBinary(column, i));
            break;
          default:
            throw new UnsupportedOperationException(
                String.format(
                    "Unsupported source dataType: %s",
                    childColumnTransformer.getType().getTypeEnum()));
        }
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  private void cast(ColumnBuilder columnBuilder, int value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, value);
        break;
      case INT64:
        returnType.writeLong(columnBuilder, value);
        break;
      case FLOAT:
        returnType.writeFloat(columnBuilder, value);
        break;
      case DOUBLE:
        returnType.writeDouble(columnBuilder, value);
        break;
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, value != 0);
        break;
      case BINARY:
        returnType.writeBinary(columnBuilder, Binary.valueOf(String.valueOf(value)));
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }

  private void cast(ColumnBuilder columnBuilder, long value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, (CastHelper.castLongToInt(value)));
        break;
      case INT64:
        returnType.writeLong(columnBuilder, value);
        break;
      case FLOAT:
        returnType.writeFloat(columnBuilder, value);
        break;
      case DOUBLE:
        returnType.writeDouble(columnBuilder, value);
        break;
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, value != 0L);
        break;
      case BINARY:
        returnType.writeBinary(columnBuilder, Binary.valueOf(String.valueOf(value)));
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }

  private void cast(ColumnBuilder columnBuilder, float value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, CastHelper.castFloatToInt(value));
        break;
      case INT64:
        returnType.writeLong(columnBuilder, CastHelper.castFloatToLong(value));
        break;
      case FLOAT:
        returnType.writeFloat(columnBuilder, value);
        break;
      case DOUBLE:
        returnType.writeDouble(columnBuilder, value);
        break;
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, value != 0.0f);
        break;
      case BINARY:
        returnType.writeBinary(columnBuilder, Binary.valueOf(String.valueOf(value)));
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }

  private void cast(ColumnBuilder columnBuilder, double value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, CastHelper.castDoubleToInt(value));
        break;
      case INT64:
        returnType.writeLong(columnBuilder, CastHelper.castDoubleToLong(value));
        break;
      case FLOAT:
        returnType.writeFloat(columnBuilder, CastHelper.castDoubleToFloat(value));
        break;
      case DOUBLE:
        returnType.writeDouble(columnBuilder, value);
        break;
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, value != 0.0);
        break;
      case BINARY:
        returnType.writeBinary(columnBuilder, Binary.valueOf(String.valueOf(value)));
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }

  private void cast(ColumnBuilder columnBuilder, boolean value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, value ? 1 : 0);
        break;
      case INT64:
        returnType.writeLong(columnBuilder, value ? 1L : 0);
        break;
      case FLOAT:
        returnType.writeFloat(columnBuilder, value ? 1.0f : 0);
        break;
      case DOUBLE:
        returnType.writeDouble(columnBuilder, value ? 1.0 : 0);
        break;
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, value);
        break;
      case BINARY:
        returnType.writeBinary(columnBuilder, Binary.valueOf(String.valueOf(value)));
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }

  private void cast(ColumnBuilder columnBuilder, Binary value) {
    String stringValue = value.getStringValue();
    switch (returnType.getTypeEnum()) {
      case INT32:
        returnType.writeInt(columnBuilder, Integer.parseInt(stringValue));
        break;
      case INT64:
        returnType.writeLong(columnBuilder, Long.parseLong(stringValue));
        break;
      case FLOAT:
        returnType.writeFloat(columnBuilder, CastHelper.castTextToFloat(stringValue));
        break;
      case DOUBLE:
        returnType.writeDouble(columnBuilder, CastHelper.castTextToDouble(stringValue));
        break;
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, CastHelper.castTextToBoolean(stringValue));
        break;
      case BINARY:
        returnType.writeBinary(columnBuilder, value);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", returnType.getTypeEnum()));
    }
  }
}

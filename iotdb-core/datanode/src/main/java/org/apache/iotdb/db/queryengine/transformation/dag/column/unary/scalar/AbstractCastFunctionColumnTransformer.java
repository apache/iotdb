/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.helper.CastFunctionHelper;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.iotdb.db.utils.DateTimeUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;

import java.time.ZoneId;
import java.time.format.DateTimeParseException;

import static org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.helper.CastFunctionHelper.ERROR_MSG;
import static org.apache.iotdb.rpc.TSStatusCode.DATE_OUT_OF_RANGE;

public abstract class AbstractCastFunctionColumnTransformer extends UnaryColumnTransformer {

  private final ZoneId zoneId;

  protected AbstractCastFunctionColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, ZoneId zoneId) {
    super(returnType, childColumnTransformer);
    this.zoneId = zoneId;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    Type childType = childColumnTransformer.getType();
    TypeEnum sourceType = childType.getTypeEnum();
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        transform(column, columnBuilder, sourceType, childType, i);
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    Type childType = childColumnTransformer.getType();
    TypeEnum sourceType = childType.getTypeEnum();
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (selection[i] && !column.isNull(i)) {
        transform(column, columnBuilder, sourceType, childType, i);
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  protected abstract void transform(
      Column column, ColumnBuilder columnBuilder, TypeEnum sourceType, Type childType, int i);

  protected void cast(ColumnBuilder columnBuilder, int value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
      case DATE:
        returnType.writeInt(columnBuilder, value);
        break;
      case INT64:
      case TIMESTAMP:
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
      case TEXT:
      case STRING:
        returnType.writeBinary(columnBuilder, BytesUtils.valueOf(String.valueOf(value)));
        break;
      case BLOB:
        returnType.writeBinary(columnBuilder, new Binary(BytesUtils.intToBytes(value)));
        break;
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, returnType.getTypeEnum()));
    }
  }

  protected void castDate(ColumnBuilder columnBuilder, int value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
      case DATE:
        returnType.writeInt(columnBuilder, value);
        break;
      case INT64:
        returnType.writeLong(columnBuilder, value);
        break;
      case TIMESTAMP:
        returnType.writeLong(
            columnBuilder,
            DateTimeUtils.correctPrecision(DateUtils.parseIntToTimestamp(value, zoneId)));
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
      case TEXT:
      case STRING:
        returnType.writeBinary(columnBuilder, BytesUtils.valueOf(DateUtils.formatDate(value)));
        break;
      case BLOB:
        returnType.writeBinary(columnBuilder, new Binary(BytesUtils.intToBytes(value)));
        break;
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, returnType.getTypeEnum()));
    }
  }

  protected void castTimestamp(ColumnBuilder columnBuilder, long value) {
    try {
      switch (returnType.getTypeEnum()) {
        case INT32:
          returnType.writeInt(columnBuilder, (CastFunctionHelper.castLongToInt(value)));
          break;
        case DATE:
          returnType.writeInt(
              columnBuilder,
              DateUtils.parseDateExpressionToInt(DateTimeUtils.convertToLocalDate(value, zoneId)));
          break;
        case INT64:
        case TIMESTAMP:
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
        case TEXT:
        case STRING:
          returnType.writeBinary(
              columnBuilder, BytesUtils.valueOf(DateTimeUtils.convertLongToDate(value, zoneId)));
          break;
        case BLOB:
          returnType.writeBinary(columnBuilder, new Binary(BytesUtils.longToBytes(value)));
          break;
        default:
          throw new UnsupportedOperationException(
              String.format(ERROR_MSG, returnType.getTypeEnum()));
      }
    } catch (DateTimeParseException e) {
      throw new IoTDBRuntimeException(
          "Year must be between 1000 and 9999.", DATE_OUT_OF_RANGE.getStatusCode(), true);
    }
  }

  protected void cast(ColumnBuilder columnBuilder, long value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
      case DATE:
        returnType.writeInt(columnBuilder, (CastFunctionHelper.castLongToInt(value)));
        break;
      case INT64:
      case TIMESTAMP:
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
      case TEXT:
      case STRING:
        returnType.writeBinary(columnBuilder, BytesUtils.valueOf(String.valueOf(value)));
        break;
      case BLOB:
        returnType.writeBinary(columnBuilder, new Binary(BytesUtils.longToBytes(value)));
        break;
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, returnType.getTypeEnum()));
    }
  }

  protected void cast(ColumnBuilder columnBuilder, float value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
      case DATE:
        returnType.writeInt(columnBuilder, CastFunctionHelper.castFloatToInt(value));
        break;
      case INT64:
      case TIMESTAMP:
        returnType.writeLong(columnBuilder, CastFunctionHelper.castFloatToLong(value));
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
      case TEXT:
      case STRING:
        returnType.writeBinary(columnBuilder, BytesUtils.valueOf(String.valueOf(value)));
        break;
      case BLOB:
        returnType.writeBinary(columnBuilder, new Binary(BytesUtils.floatToBytes(value)));
        break;
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, returnType.getTypeEnum()));
    }
  }

  protected void cast(ColumnBuilder columnBuilder, double value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
      case DATE:
        returnType.writeInt(columnBuilder, CastFunctionHelper.castDoubleToInt(value));
        break;
      case INT64:
      case TIMESTAMP:
        returnType.writeLong(columnBuilder, CastFunctionHelper.castDoubleToLong(value));
        break;
      case FLOAT:
        returnType.writeFloat(columnBuilder, CastFunctionHelper.castDoubleToFloat(value));
        break;
      case DOUBLE:
        returnType.writeDouble(columnBuilder, value);
        break;
      case BOOLEAN:
        returnType.writeBoolean(columnBuilder, value != 0.0);
        break;
      case TEXT:
      case STRING:
        returnType.writeBinary(columnBuilder, BytesUtils.valueOf(String.valueOf(value)));
        break;
      case BLOB:
        returnType.writeBinary(columnBuilder, new Binary(BytesUtils.doubleToBytes(value)));
        break;
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, returnType.getTypeEnum()));
    }
  }

  protected void cast(ColumnBuilder columnBuilder, boolean value) {
    switch (returnType.getTypeEnum()) {
      case INT32:
      case DATE:
        returnType.writeInt(columnBuilder, value ? 1 : 0);
        break;
      case INT64:
      case TIMESTAMP:
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
      case TEXT:
      case STRING:
        returnType.writeBinary(columnBuilder, BytesUtils.valueOf(String.valueOf(value)));
        break;
      case BLOB:
        returnType.writeBinary(columnBuilder, new Binary(BytesUtils.boolToBytes(value)));
        break;
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, returnType.getTypeEnum()));
    }
  }

  protected void cast(ColumnBuilder columnBuilder, Binary value) {
    String stringValue = value.getStringValue(TSFileConfig.STRING_CHARSET);
    try {
      switch (returnType.getTypeEnum()) {
        case INT32:
          returnType.writeInt(columnBuilder, Integer.parseInt(stringValue));
          break;
        case DATE:
          returnType.writeInt(columnBuilder, DateUtils.parseDateExpressionToInt(stringValue));
          break;
        case INT64:
          returnType.writeLong(columnBuilder, Long.parseLong(stringValue));
          break;
        case TIMESTAMP:
          returnType.writeLong(
              columnBuilder, DateTimeUtils.convertDatetimeStrToLong(stringValue, zoneId));
          break;
        case FLOAT:
          returnType.writeFloat(columnBuilder, CastFunctionHelper.castTextToFloat(stringValue));
          break;
        case DOUBLE:
          returnType.writeDouble(columnBuilder, CastFunctionHelper.castTextToDouble(stringValue));
          break;
        case BOOLEAN:
          returnType.writeBoolean(columnBuilder, CastFunctionHelper.castTextToBoolean(stringValue));
          break;
        case TEXT:
        case STRING:
        case BLOB:
          returnType.writeBinary(columnBuilder, value);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format(ERROR_MSG, returnType.getTypeEnum()));
      }
    } catch (DateTimeParseException | NumberFormatException e) {
      throw new SemanticException(
          String.format("Cannot cast %s to %s type", stringValue, returnType.getDisplayName()));
    }
  }
}

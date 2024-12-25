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
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;

import java.time.ZoneId;

public class TryCastFunctionColumnTransformer extends AbstractCastFunctionColumnTransformer {

  public TryCastFunctionColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, ZoneId zoneId) {
    super(returnType, childColumnTransformer, zoneId);
  }

  @Override
  protected void transform(
      Column column, ColumnBuilder columnBuilder, TypeEnum sourceType, Type childType, int i) {
    try {
      switch (sourceType) {
        case INT32:
          cast(columnBuilder, childType.getInt(column, i));
          break;
        case DATE:
          castDate(columnBuilder, childType.getInt(column, i));
          break;
        case INT64:
          cast(columnBuilder, childType.getLong(column, i));
          break;
        case TIMESTAMP:
          castTimestamp(columnBuilder, childType.getLong(column, i));
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
        case TEXT:
        case STRING:
        case BLOB:
          cast(columnBuilder, childType.getBinary(column, i));
          break;
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "Unsupported source dataType: %s",
                  childColumnTransformer.getType().getTypeEnum()));
      }
    } catch (IoTDBRuntimeException e) {
      columnBuilder.appendNull();
    }
  }
}

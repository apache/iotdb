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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.BinaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;

public class RoundColumnTransformer extends BinaryColumnTransformer {

  public RoundColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    TypeEnum sourceType = leftTransformer.getType().getTypeEnum();
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        transform(sourceType, leftColumn, rightColumn, builder, i);
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn,
      Column rightColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection) {
    TypeEnum sourceType = leftTransformer.getType().getTypeEnum();
    for (int i = 0; i < positionCount; i++) {
      if (selection[i] && !leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        transform(sourceType, leftColumn, rightColumn, builder, i);
      } else {
        builder.appendNull();
      }
    }
  }

  private void transform(
      TypeEnum sourceType, Column leftColumn, Column rightColumn, ColumnBuilder builder, int i) {
    int places = rightTransformer.getType().getInt(rightColumn, i);
    switch (sourceType) {
      case INT32:
        builder.writeDouble(
            Math.rint(leftColumn.getInt(i) * Math.pow(10, places)) / Math.pow(10, places));
        break;
      case INT64:
        builder.writeDouble(
            Math.rint(leftColumn.getLong(i) * Math.pow(10, places)) / Math.pow(10, places));
        break;
      case FLOAT:
        builder.writeDouble(
            Math.rint(leftColumn.getFloat(i) * Math.pow(10, places)) / Math.pow(10, places));
        break;
      case DOUBLE:
        builder.writeDouble(
            Math.rint(leftColumn.getDouble(i) * Math.pow(10, places)) / Math.pow(10, places));
        break;
      case DATE:
      case TEXT:
      case BOOLEAN:
      case BLOB:
      case STRING:
      case TIMESTAMP:
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported source dataType: %s", sourceType));
    }
  }

  @Override
  protected void checkType() {
    if (!leftTransformer.isReturnTypeNumeric() || !rightTransformer.isReturnTypeNumeric()) {
      throw new UnsupportedOperationException("Unsupported Type");
    }
  }
}

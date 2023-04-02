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

package org.apache.iotdb.db.mpp.transformation.dag.column.binary;

import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.BinaryType;
import org.apache.iotdb.tsfile.read.common.type.BooleanType;
import org.apache.iotdb.tsfile.read.common.type.DoubleType;
import org.apache.iotdb.tsfile.read.common.type.FloatType;
import org.apache.iotdb.tsfile.read.common.type.IntType;
import org.apache.iotdb.tsfile.read.common.type.LongType;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;

public class WhenThenColumnTransformer extends BinaryColumnTransformer {
  public WhenThenColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  public ColumnTransformer getWhen() {
    return leftTransformer;
  }

  public ColumnTransformer getThen() {
    return rightTransformer;
  }

  @Override
  protected void checkType() {
    if (getWhen().typeNotEquals(TypeEnum.BOOLEAN)) {
      throw new UnsupportedOperationException(
          "Unsupported Type, WHEN expression must return boolean");
    }
  }

  private void writeToColumnBuilder(
      ColumnTransformer childTransformer, Column column, int index, ColumnBuilder builder) {
    if (returnType instanceof BooleanType) {
      builder.writeBoolean(childTransformer.getType().getBoolean(column, index));
    } else if (returnType instanceof IntType) {
      builder.writeInt(childTransformer.getType().getInt(column, index));
    } else if (returnType instanceof LongType) {
      builder.writeLong(childTransformer.getType().getLong(column, index));
    } else if (returnType instanceof FloatType) {
      builder.writeFloat(childTransformer.getType().getFloat(column, index));
    } else if (returnType instanceof DoubleType) {
      builder.writeDouble(childTransformer.getType().getDouble(column, index));
    } else if (returnType instanceof BinaryType) {
      builder.writeBinary(childTransformer.getType().getBinary(column, index));
    } else {
      throw new UnsupportedOperationException("Unsupported Type");
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        boolean whenResult = getWhen().getType().getBoolean(leftColumn, i);
        if (whenResult) {
          Type rightType = getThen().getType();
          writeToColumnBuilder(getThen(), rightColumn, i, builder);
          //          builder.writeObject(rightType.getObject(rightColumn, i)); // which type should
          // I write?
        } else {
          builder.appendNull();
        }
      } else {
        builder.appendNull();
      }
    }
  }
}

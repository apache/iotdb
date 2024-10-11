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
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

public class ConcatColumnTransformer extends UnaryColumnTransformer {
  private final byte[] value;
  private final boolean isBehind;

  public ConcatColumnTransformer(
      Type returnType,
      ColumnTransformer childColumnTransformer,
      String valueStr,
      boolean isBehind) {
    super(returnType, childColumnTransformer);
    this.value = valueStr.getBytes();
    this.isBehind = isBehind;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      transform(column, columnBuilder, i);
    }
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (selection[i]) {
        transform(column, columnBuilder, i);
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  private void transform(Column column, ColumnBuilder columnBuilder, int i) {
    if (!column.isNull(i)) {
      if (isBehind) {
        columnBuilder.writeBinary(new Binary(concat(column.getBinary(i).getValues(), value)));
      } else {
        columnBuilder.writeBinary(new Binary(concat(value, column.getBinary(i).getValues())));
      }
    } else {
      columnBuilder.writeBinary(new Binary(value));
    }
  }

  // 只处理两个参数的concat
  public static byte[] concat(byte[] leftValue, byte[] rightValue) {
    byte[] result = new byte[leftValue.length + rightValue.length];
    System.arraycopy(leftValue, 0, result, 0, leftValue.length);
    System.arraycopy(rightValue, 0, result, leftValue.length, rightValue.length);
    return result;
  }
}

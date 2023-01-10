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
import org.apache.iotdb.db.mpp.transformation.dag.util.TransformUtils;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;

public abstract class CompareBinaryColumnTransformer extends BinaryColumnTransformer {

  protected CompareBinaryColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        boolean flag;
        // compare binary type
        if (leftTransformer.getType().getTypeEnum().equals(TypeEnum.BINARY)) {
          flag =
              transform(
                  TransformUtils.compare(
                      leftTransformer.getType().getBinary(leftColumn, i).getStringValue(),
                      rightTransformer.getType().getBinary(rightColumn, i).getStringValue()));
        } else if (leftTransformer.getType().getTypeEnum().equals(TypeEnum.BOOLEAN)) {
          flag =
              transform(
                  Boolean.compare(
                      leftTransformer.getType().getBoolean(leftColumn, i),
                      rightTransformer.getType().getBoolean(rightColumn, i)));
        } else {
          double left = leftTransformer.getType().getDouble(leftColumn, i);
          double right = rightTransformer.getType().getDouble(rightColumn, i);
          if (Double.isNaN(left) || Double.isNaN(right)) {
            flag = false;
          } else {
            flag = transform(compare(left, right));
          }
        }
        returnType.writeBoolean(builder, flag);
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void checkType() {
    // Boolean type can only be compared by == or !=
    if (leftTransformer.typeNotEquals(TypeEnum.BOOLEAN)
        && rightTransformer.typeNotEquals(TypeEnum.BOOLEAN)) {
      return;
    }
    throw new UnsupportedOperationException("Unsupported Type");
  }

  protected int compare(double d1, double d2) {
    return Double.compare(d1, d2);
  }

  /**
   * transform int value of flag to corresponding boolean value
   *
   * @param flag
   * @return
   */
  protected abstract boolean transform(int flag);
}

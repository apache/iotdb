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

public class DiffColumnTransformer extends BinaryColumnTransformer {

  // cache the last non-null value
  private double lastValue;

  // indicate whether lastValue is null
  private boolean lastValueIsNull = true;

  public DiffColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    Type leftType = leftTransformer.getType();
    Type rightType = rightTransformer.getType();
    for (int i = 0; i < positionCount; i++) {
      if (leftColumn.isNull(i)) {
        // currValue is null, append null
        builder.appendNull();

        // When currValue is null:
        // ignoreNull = true, keep lastValueIsNull as before
        // ignoreNull = false or ignoreNull is null, update lastValueIsNull to true
        lastValueIsNull |= (rightColumn.isNull(i) || !rightType.getBoolean(rightColumn, i));
      } else {
        double currValue = leftType.getDouble(leftColumn, i);
        if (lastValueIsNull) {
          builder.appendNull(); // lastValue is null, append null
        } else {
          returnType.writeDouble(builder, currValue - lastValue);
        }

        // currValue is not null, update lastValue
        lastValue = currValue;
        lastValueIsNull = false;
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
    // Diff do not support short circuit evaluation
    doTransform(leftColumn, rightColumn, builder, positionCount);
  }
}

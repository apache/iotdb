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

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;

public abstract class ArithmeticBinaryColumnTransformer extends BinaryColumnTransformer {
  public ArithmeticBinaryColumnTransformer(
      Expression expression,
      Type returnType,
      ColumnTransformer leftTransformer,
      ColumnTransformer rightTransformer) {
    super(expression, returnType, leftTransformer, rightTransformer);
  }

  @Override
  public void evaluate() {
    leftTransformer.tryEvaluate();
    rightTransformer.tryEvaluate();
    Column leftColumn = leftTransformer.getColumn();
    Column rightColumn = rightTransformer.getColumn();
    int size = leftColumn.getPositionCount();
    ColumnBuilder builder = returnType.createBlockBuilder(size);
    for (int i = 0; i < size; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        returnType.writeDouble(
            builder,
            transform(
                leftTransformer.getType().getDouble(leftColumn, i),
                rightTransformer.getType().getDouble(rightColumn, i)));
      } else {
        returnType.appendNull(builder);
      }
    }
    initializeColumnCache(builder.build());
  }

  protected abstract double transform(double d1, double d2);
}

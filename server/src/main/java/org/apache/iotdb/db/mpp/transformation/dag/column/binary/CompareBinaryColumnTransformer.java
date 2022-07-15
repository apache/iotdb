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
import org.apache.iotdb.tsfile.read.common.type.BinaryType;
import org.apache.iotdb.tsfile.read.common.type.Type;

import java.util.Objects;

public abstract class CompareBinaryColumnTransformer extends BinaryColumnTransformer {

  public CompareBinaryColumnTransformer(
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
    ColumnBuilder builder = returnType.createColumnBuilder(size);
    for (int i = 0; i < size; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        boolean flag;
        // compare binary type
        if (leftTransformer.getType() instanceof BinaryType) {
          flag =
              transform(
                  compare(
                      leftTransformer.getType().getBinary(leftColumn, i).getStringValue(),
                      rightTransformer.getType().getBinary(rightColumn, i).getStringValue()));
        } else {
          flag =
              transform(
                  compare(
                      leftTransformer.getType().getDouble(leftColumn, i),
                      rightTransformer.getType().getDouble(rightColumn, i)));
        }
        returnType.writeBoolean(builder, flag);
      } else {
        builder.appendNull();
      }
    }
    initializeColumnCache(builder.build());
  }

  protected int compare(CharSequence cs1, CharSequence cs2) {
    if (Objects.requireNonNull(cs1) == Objects.requireNonNull(cs2)) {
      return 0;
    }

    if (cs1.getClass() == cs2.getClass() && cs1 instanceof Comparable) {
      return ((Comparable<Object>) cs1).compareTo(cs2);
    }

    for (int i = 0, len = Math.min(cs1.length(), cs2.length()); i < len; i++) {
      char a = cs1.charAt(i);
      char b = cs2.charAt(i);
      if (a != b) {
        return a - b;
      }
    }

    return cs1.length() - cs2.length();
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

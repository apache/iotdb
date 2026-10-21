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

package org.apache.iotdb.calc.transformation.dag.column.unary.scalar;

import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.UnaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

public class DiffFunctionColumnTransformer extends UnaryColumnTransformer {

  // default is true
  private final boolean ignoreNull;

  // cache the last non-null value
  private double lastValue;

  // indicate whether lastValue is null
  private boolean lastValueIsNull = true;

  // DIFF must advance its state once per input, even when several masks/consumers request it.
  private Column currentInputResult;

  public DiffFunctionColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, boolean ignoreNull) {
    super(returnType, childColumnTransformer);
    this.ignoreNull = ignoreNull;
  }

  @Override
  public void evaluate() {
    initializeColumnCache(evaluateCurrentInput());
  }

  @Override
  protected Column evaluateSelection(boolean[] selection) {
    // DIFF depends on all preceding rows, including rows rejected by a CASE/filter mask.
    return evaluateCurrentInput();
  }

  private Column evaluateCurrentInput() {
    if (currentInputResult == null) {
      childColumnTransformer.tryEvaluate();
      Column column = childColumnTransformer.getColumn();
      ColumnBuilder builder = returnType.createColumnBuilder(column.getPositionCount());
      doTransform(column, builder);
      currentInputResult = builder.build();
    }
    return currentInputResult;
  }

  @Override
  protected void resetForNewInput() {
    currentInputResult = null;
  }

  @Override
  public void close() {
    currentInputResult = null;
    super.close();
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (column.isNull(i)) {
        columnBuilder.appendNull(); // currValue is null, append null

        // When currValue is null:
        // ignoreNull = true, keep lastValueIsNull as before
        // ignoreNull = false, update lastValueIsNull to true
        lastValueIsNull |= !ignoreNull;
      } else {
        double currValue = childColumnTransformer.getType().getDouble(column, i);
        if (lastValueIsNull) {
          // lastValue is null, append null
          columnBuilder.appendNull();
        } else {
          returnType.writeDouble(columnBuilder, currValue - lastValue);
        }

        // currValue is not null, update lastValue
        lastValue = currValue;
        lastValueIsNull = false;
      }
    }
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    // Diff do not support short circuit evaluation
    doTransform(column, columnBuilder);
  }
}

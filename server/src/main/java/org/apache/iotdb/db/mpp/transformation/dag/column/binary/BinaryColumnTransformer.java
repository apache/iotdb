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

public abstract class BinaryColumnTransformer extends ColumnTransformer {

  protected final ColumnTransformer leftTransformer;

  protected final ColumnTransformer rightTransformer;

  public BinaryColumnTransformer(
      Expression expression,
      Type returnType,
      ColumnTransformer leftTransformer,
      ColumnTransformer rightTransformer) {
    super(expression, returnType);
    this.leftTransformer = leftTransformer;
    this.rightTransformer = rightTransformer;
  }

  @Override
  public void evaluate() {
    leftTransformer.tryEvaluate();
    rightTransformer.tryEvaluate();
    Column leftColumn = leftTransformer.getColumn();
    Column rightColumn = rightTransformer.getColumn();
    ColumnBuilder builder = returnType.createColumnBuilder(leftColumn.getPositionCount());
    doTransform(leftColumn, rightColumn, builder);
    initializeColumnCache(builder.build());
  }

  @Override
  public void reset() {
    hasEvaluated = false;
    if (leftTransformer != null) {
      leftTransformer.reset();
    }
    if (rightTransformer != null) {
      rightTransformer.reset();
    }
  }

  protected abstract void doTransform(Column leftColumn, Column rightColumn, ColumnBuilder builder);

  public ColumnTransformer getLeftTransformer() {
    return leftTransformer;
  }

  public ColumnTransformer getRightTransformer() {
    return rightTransformer;
  }
}

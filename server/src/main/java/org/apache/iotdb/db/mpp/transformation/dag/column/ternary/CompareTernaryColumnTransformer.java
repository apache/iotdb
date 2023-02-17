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

package org.apache.iotdb.db.mpp.transformation.dag.column.ternary;

import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;

public abstract class CompareTernaryColumnTransformer extends TernaryColumnTransformer {
  protected CompareTernaryColumnTransformer(
      Type returnType,
      ColumnTransformer firstColumnTransformer,
      ColumnTransformer secondColumnTransformer,
      ColumnTransformer thirdColumnTransformer) {
    super(returnType, firstColumnTransformer, secondColumnTransformer, thirdColumnTransformer);
  }

  @Override
  public void evaluate() {
    firstColumnTransformer.tryEvaluate();
    secondColumnTransformer.tryEvaluate();
    thirdColumnTransformer.tryEvaluate();
    // attention: get positionCount before calling getColumn
    int positionCount = firstColumnTransformer.getColumnCachePositionCount();
    Column firstColumn = firstColumnTransformer.getColumn();
    Column secondColumn = secondColumnTransformer.getColumn();
    Column thirdColumn = thirdColumnTransformer.getColumn();
    ColumnBuilder columnBuilder = returnType.createColumnBuilder(positionCount);
    doTransform(firstColumn, secondColumn, thirdColumn, columnBuilder, positionCount);
    initializeColumnCache(columnBuilder.build());
  }

  @Override
  protected final void checkType() {
    if (firstColumnTransformer.isReturnTypeNumeric()
            && secondColumnTransformer.isReturnTypeNumeric()
            && thirdColumnTransformer.isReturnTypeNumeric()
        || firstColumnTransformer.typeEquals(TypeEnum.BINARY)
            && secondColumnTransformer.typeEquals(TypeEnum.BINARY)
            && thirdColumnTransformer.typeEquals(TypeEnum.BINARY)) {
      return;
    }

    throw new UnsupportedOperationException(
        "The Type of three subExpression should be all Numeric or Text");
  }

  protected abstract void doTransform(
      Column firstColumn,
      Column secondColumn,
      Column thirdColumn,
      ColumnBuilder builder,
      int positionCount);
}

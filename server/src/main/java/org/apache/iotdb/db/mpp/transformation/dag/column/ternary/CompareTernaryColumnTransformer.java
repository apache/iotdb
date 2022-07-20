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

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;

public abstract class CompareTernaryColumnTransformer extends TernaryColumnTransformer {
  public CompareTernaryColumnTransformer(
      Expression expression,
      Type returnType,
      ColumnTransformer firstColumnTransformer,
      ColumnTransformer secondColumnTransformer,
      ColumnTransformer thirdColumnTransformer) {
    super(
        expression,
        returnType,
        firstColumnTransformer,
        secondColumnTransformer,
        thirdColumnTransformer);
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
    if (firstColumnTransformer == null
        || secondColumnTransformer == null
        || thirdColumnTransformer == null) {
      return;
    }
    if ((firstColumnTransformer.getTsDataType()).equals(secondColumnTransformer.getTsDataType())
        && (firstColumnTransformer.getTsDataType())
            .equals(thirdColumnTransformer.getTsDataType())) {
      return;
    }

    if (firstColumnTransformer.getTsDataType().equals(TSDataType.BOOLEAN)
        || secondColumnTransformer.getTsDataType().equals(TSDataType.BOOLEAN)
        || thirdColumnTransformer.getTsDataType().equals(TSDataType.BOOLEAN)) {
      throw new UnSupportedDataTypeException(TSDataType.BOOLEAN.toString());
    }
  }

  protected abstract void doTransform(
      Column firstColumn,
      Column secondColumn,
      Column thirdColumn,
      ColumnBuilder builder,
      int positionCount);
}

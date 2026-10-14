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

package org.apache.iotdb.calc.transformation.dag.column.multi;

import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.udf.UDTFExecutor;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.util.function.Consumer;

public class MappableUDFColumnTransformer extends ColumnTransformer {

  private final ColumnTransformer[] inputColumnTransformers;

  private final UDTFExecutor executor;

  public MappableUDFColumnTransformer(
      Type returnType, ColumnTransformer[] inputColumnTransformers, UDTFExecutor executor) {
    super(returnType);
    this.inputColumnTransformers = inputColumnTransformers;
    this.executor = executor;
  }

  @Override
  public void evaluate() {
    // pull columns from previous transformers
    Column[] columns = new Column[inputColumnTransformers.length];
    for (int i = 0; i < inputColumnTransformers.length; i++) {
      inputColumnTransformers[i].tryEvaluate();
      // Retain each input column before evaluating the next sibling.
      columns[i] = inputColumnTransformers[i].getColumn();
    }
    initializeColumnCache(doTransform(columns));
  }

  @Override
  protected Column evaluateSelection(boolean[] selection) {
    int[] positions = new int[selection.length];
    int selectedCount = 0;
    for (int i = 0; i < selection.length; i++) {
      if (selection[i]) {
        positions[selectedCount++] = i;
      }
    }
    Column[] columns = new Column[inputColumnTransformers.length];
    for (int i = 0; i < inputColumnTransformers.length; i++) {
      columns[i] =
          inputColumnTransformers[i]
              .getColumnWithSelection(selection)
              .getPositions(positions, 0, selectedCount);
    }
    ColumnBuilder builder = returnType.createColumnBuilder(selection.length);
    // A mappable UDF only sees selected rows, even when its inputs reuse full cached columns.
    Column result = selectedCount == 0 ? null : doTransform(columns);
    int resultIndex = 0;
    for (boolean selected : selection) {
      if (selected) {
        if (result.isNull(resultIndex)) {
          builder.appendNull();
        } else {
          builder.write(result, resultIndex);
        }
        resultIndex++;
      } else {
        builder.appendNull();
      }
    }
    return builder.build();
  }

  private Column doTransform(Column[] columns) {
    // construct input TsBlock with columns
    int positionCount = columns[0].getPositionCount();
    ColumnBuilder builder = returnType.createColumnBuilder(positionCount);
    // executor UDF and cache result
    executor.execute(columns, builder);
    return builder.build();
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  public ColumnTransformer[] getInputColumnTransformers() {
    return inputColumnTransformers;
  }

  @Override
  public void close() {
    super.close();
    for (ColumnTransformer inputColumnTransformer : inputColumnTransformers) {
      inputColumnTransformer.close();
    }
    // finalize executor
    executor.beforeDestroy();
  }

  @Override
  protected void forEachChild(Consumer<ColumnTransformer> action) {
    for (ColumnTransformer columnTransformer : inputColumnTransformers) {
      action.accept(columnTransformer);
    }
  }
}

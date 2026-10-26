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
import org.apache.iotdb.calc.transformation.dag.column.binary.BinaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.util.Arrays;

public class DiffColumnTransformer extends BinaryColumnTransformer {

  // cache the last non-null value
  private double lastValue;

  // indicate whether lastValue is null
  private boolean lastValueIsNull = true;

  // Re-evaluation in the same block must start from the state before this input.
  private double lastValueBeforeCurrentInput;
  private boolean lastValueIsNullBeforeCurrentInput = true;

  // A later masked pass must not replace the state carried by a complete evaluation.
  private double lastValueAfterFullEvaluation;
  private boolean lastValueIsNullAfterFullEvaluation;
  private boolean hasFullEvaluation;

  // Schema UPDATE keeps the filter's results until its SET expressions have consumed them.
  private boolean retainResults;
  private boolean reuseRetainedResults;
  private Column retainedResult;
  private boolean[] retainedRows;
  private boolean[] evaluationSelection;

  public DiffColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  @Override
  protected void resetForNewInput() {
    lastValueBeforeCurrentInput = lastValue;
    lastValueIsNullBeforeCurrentInput = lastValueIsNull;
    hasFullEvaluation = false;
    releaseRetainedResult();
  }

  @Override
  public boolean canEvaluateWithSelection() {
    // A fresh masked transform changes the running state. Schema UPDATE explicitly opts into
    // reusing the filter's results instead of treating DIFF as a stateless expression.
    return false;
  }

  /** Enables result retention for a schema UPDATE's filter and SET phases. */
  public void retainResultForReuse() {
    retainResults = true;
  }

  /** Starts the SET phase without evaluating rows excluded by the filter's CASE guards. */
  public void reuseRetainedResult() {
    reuseRetainedResults = true;
    columnCache.clear();
  }

  /** Releases this batch's columns while keeping the state produced by its filter. */
  public void releaseRetainedResult() {
    retainedResult = null;
    retainedRows = null;
    reuseRetainedResults = false;
    evaluationSelection = null;
  }

  @Override
  public void initializeColumnCache(Column column) {
    super.initializeColumnCache(column);
    if (!retainResults || reuseRetainedResults) {
      return;
    }
    if (retainedResult == null || evaluationSelection == null) {
      retainedResult = column;
      retainedRows = new boolean[column.getPositionCount()];
      if (evaluationSelection == null) {
        Arrays.fill(retainedRows, true);
      } else {
        System.arraycopy(evaluationSelection, 0, retainedRows, 0, retainedRows.length);
      }
      return;
    }
    ColumnBuilder builder = returnType.createColumnBuilder(column.getPositionCount());
    for (int i = 0; i < column.getPositionCount(); i++) {
      appendValue(builder, evaluationSelection[i] ? column : retainedResult, i);
      retainedRows[i] |= evaluationSelection[i];
    }
    retainedResult = builder.build();
  }

  @Override
  public void evaluate() {
    if (reuseRetainedResults) {
      evaluateRetainedResult(null);
    } else {
      super.evaluate();
    }
  }

  @Override
  public void evaluateWithSelection(boolean[] selection) {
    if (reuseRetainedResults) {
      evaluateRetainedResult(selection);
      return;
    }
    evaluationSelection = selection;
    try {
      super.evaluateWithSelection(selection);
    } finally {
      evaluationSelection = null;
    }
  }

  private void evaluateRetainedResult(boolean[] selection) {
    boolean missing = retainedResult == null;
    if (!missing) {
      for (int i = 0; i < retainedRows.length; i++) {
        if (!retainedRows[i] && (selection == null || selection[i])) {
          missing = true;
          break;
        }
      }
    }
    if (missing) {
      Column previousResult = retainedResult;
      boolean[] previousRows = retainedRows == null ? null : retainedRows.clone();
      boolean[] expandedSelection = selection == null ? null : selection.clone();
      if (expandedSelection != null && previousRows != null) {
        for (int i = 0; i < expandedSelection.length; i++) {
          expandedSelection[i] |= previousRows[i];
        }
      }
      double savedLastValue = lastValue;
      boolean savedLastValueIsNull = lastValueIsNull;
      double savedFullValue = lastValueAfterFullEvaluation;
      boolean savedFullValueIsNull = lastValueIsNullAfterFullEvaluation;
      boolean savedHasFullEvaluation = hasFullEvaluation;
      reuseRetainedResults = false;
      evaluationSelection = expandedSelection;
      try {
        if (expandedSelection == null) {
          super.evaluate();
        } else {
          super.evaluateWithSelection(expandedSelection);
        }
      } finally {
        reuseRetainedResults = true;
        evaluationSelection = null;
        lastValue = savedLastValue;
        lastValueIsNull = savedLastValueIsNull;
        lastValueAfterFullEvaluation = savedFullValue;
        lastValueIsNullAfterFullEvaluation = savedFullValueIsNull;
        hasFullEvaluation = savedHasFullEvaluation;
      }
      // An additional SET consumer must not change values already used by WHERE.
      if (previousResult != null) {
        ColumnBuilder builder = returnType.createColumnBuilder(retainedRows.length);
        for (int i = 0; i < retainedRows.length; i++) {
          appendValue(builder, previousRows[i] ? previousResult : retainedResult, i);
        }
        retainedResult = builder.build();
      }
    }
    initializeColumnCache(retainedResult);
  }

  private static void appendValue(ColumnBuilder builder, Column column, int index) {
    if (column.isNull(index)) {
      builder.appendNull();
    } else {
      builder.write(column, index);
    }
  }

  @Override
  public void close() {
    releaseRetainedResult();
    super.close();
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    transform(leftColumn, rightColumn, builder, positionCount, true);
  }

  private void transform(
      Column leftColumn,
      Column rightColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean fullEvaluation) {
    lastValue = lastValueBeforeCurrentInput;
    lastValueIsNull = lastValueIsNullBeforeCurrentInput;
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

    if (fullEvaluation) {
      lastValueAfterFullEvaluation = lastValue;
      lastValueIsNullAfterFullEvaluation = lastValueIsNull;
      hasFullEvaluation = true;
    } else if (hasFullEvaluation) {
      lastValue = lastValueAfterFullEvaluation;
      lastValueIsNull = lastValueIsNullAfterFullEvaluation;
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn,
      Column rightColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection) {
    boolean fullEvaluation = true;
    for (boolean selected : selection) {
      if (!selected) {
        fullEvaluation = false;
        break;
      }
    }
    // Preserve the selected-input results, but retain a complete pass's cross-block state.
    transform(leftColumn, rightColumn, builder, positionCount, fullEvaluation);
  }
}

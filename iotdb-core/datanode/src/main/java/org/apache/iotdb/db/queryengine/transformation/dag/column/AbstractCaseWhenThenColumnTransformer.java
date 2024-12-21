/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.transformation.dag.column;

import org.apache.commons.lang3.Validate;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractCaseWhenThenColumnTransformer extends ColumnTransformer {

  List<Pair<ColumnTransformer, ColumnTransformer>> whenThenTransformers;
  ColumnTransformer elseTransformer;

  protected AbstractCaseWhenThenColumnTransformer(
      Type returnType,
      List<ColumnTransformer> whenTransformers,
      List<ColumnTransformer> thenTransformers,
      ColumnTransformer elseTransformer) {
    super(returnType);
    Validate.isTrue(
        whenTransformers.size() == thenTransformers.size(),
        "the size between whenTransformers and thenTransformers needs to be same");
    this.whenThenTransformers = new ArrayList<>();
    for (int i = 0; i < whenTransformers.size(); i++) {
      this.whenThenTransformers.add(new Pair<>(whenTransformers.get(i), thenTransformers.get(i)));
    }
    this.elseTransformer = elseTransformer;
  }

  public List<Pair<ColumnTransformer, ColumnTransformer>> getWhenThenColumnTransformers() {
    return whenThenTransformers;
  }

  public ColumnTransformer getElseTransformer() {
    return elseTransformer;
  }

  @Override
  public void evaluate() {

    List<Column> thenColumnList = new ArrayList<>();

    // region evaluate first when
    ColumnTransformer firstWhenColumnTransformer = whenThenTransformers.get(0).left;
    firstWhenColumnTransformer.evaluate();
    Column firstWhenColumn = firstWhenColumnTransformer.getColumn();

    int positionCount = firstWhenColumn.getPositionCount();
    boolean[] selection = new boolean[positionCount];
    Arrays.fill(selection, true);

    int[] branchIndexForEachRow = new int[positionCount];
    Arrays.fill(branchIndexForEachRow, -1);

    boolean[] selectionForThen = selection.clone();

    // Update branchIndexForEachRow based on first whenColumn
    for (int i = 0; i < positionCount; i++) {
      // Update selectionForThen and branchIndexForEachRow when there is no matching whenTransformer
      // for the current row.
      if (branchIndexForEachRow[i] == -1) {
        if (!firstWhenColumn.isNull(i) && firstWhenColumn.getBoolean(i)) {
          branchIndexForEachRow[i] = 0;
          selectionForThen[i] = true;
          selection[i] = false;
        } else {
          selectionForThen[i] = false;
        }
      }
    }

    ColumnTransformer firstThenColumnTransformer = whenThenTransformers.get(0).right;
    firstThenColumnTransformer.evaluateWithSelection(selectionForThen);
    Column firstThenColumn = firstThenColumnTransformer.getColumn();
    thenColumnList.add(firstThenColumn);

    // endregion

    // when and then columns
    for (int i = 1; i < whenThenTransformers.size(); i++) {
      ColumnTransformer whenColumnTransformer = whenThenTransformers.get(i).left;
      whenColumnTransformer.evaluateWithSelection(selection);
      Column whenColumn = whenColumnTransformer.getColumn();

      selectionForThen = selection.clone();

      for (int j = 0; j < positionCount; j++) {
        if (selection[j] && !whenColumn.isNull(j) && whenColumn.getBoolean(j)) {
          branchIndexForEachRow[j] = i;
          selectionForThen[j] = true;
          // also update selection array
          selection[j] = false;
        } else {
          selectionForThen[j] = false;
        }
      }

      ColumnTransformer thenColumnTransformer = whenThenTransformers.get(i).right;
      thenColumnTransformer.evaluateWithSelection(selectionForThen);
      Column thenColumn = thenColumnTransformer.getColumn();
      thenColumnList.add(thenColumn);
    }

    // elseColumn
    doTransform(branchIndexForEachRow, positionCount, thenColumnList);
  }

  @Override
  public void evaluateWithSelection(boolean[] selection) {

    // region initialize branchIndexForEachRow.
    // branchIndexForEachRow indicates the index of the WhenTransformer matched by each row.
    int[] branchIndexForEachRow = new int[selection.length];
    // positionCount indicates the length of column
    int positionCount = selection.length;
    boolean[] selectionForWhen = selection.clone();

    // An assignment of -1 means that evaluation is required, otherwise it means that evaluation is
    // not required.
    for (int i = 0; i < selection.length; i++) {
      if (selection[i]) {
        branchIndexForEachRow[i] = -1;
      } else {
        branchIndexForEachRow[i] = whenThenTransformers.size();
      }
    }
    // endregion

    List<Column> thenColumnList = new ArrayList<>();

    // when and then columns
    for (int i = 0; i < whenThenTransformers.size(); i++) {
      ColumnTransformer whenColumnTransformer = whenThenTransformers.get(i).left;
      whenColumnTransformer.evaluateWithSelection(selectionForWhen);
      Column whenColumn = whenColumnTransformer.getColumn();

      boolean[] selectionForThen = selectionForWhen.clone();

      for (int j = 0; j < positionCount; j++) {
        if (selectionForWhen[j] && !whenColumn.isNull(j) && whenColumn.getBoolean(j)) {
          branchIndexForEachRow[j] = i;
          selectionForThen[j] = true;
          // also update selection array
          selectionForWhen[j] = false;
        } else {
          selectionForThen[j] = false;
        }
      }

      ColumnTransformer thenColumnTransformer = whenThenTransformers.get(i).right;
      thenColumnTransformer.evaluateWithSelection(selectionForThen);
      Column thenColumn = thenColumnTransformer.getColumn();
      thenColumnList.add(thenColumn);
    }

    // elseColumn
    // when selectionForElse[i] is false represent the rows that do not need to evaluate
    doTransform(branchIndexForEachRow, positionCount, thenColumnList);
  }

  private void doTransform(
      int[] branchIndexForEachRow, int positionCount, List<Column> thenColumnList) {
    boolean[] selectionForElse = new boolean[positionCount];
    for (int i = 0; i < branchIndexForEachRow.length; i++) {
      if (branchIndexForEachRow[i] == -1) {
        selectionForElse[i] = true;
      }
    }
    elseTransformer.evaluateWithSelection(selectionForElse);

    ColumnBuilder builder = returnType.createColumnBuilder(positionCount);
    Column elseColumn = elseTransformer.getColumn();

    for (int i = 0; i < positionCount; i++) {
      Column resultColumn = null;
      Type thenColumnType = null;
      if (branchIndexForEachRow[i] == -1) {
        resultColumn = elseColumn;
        thenColumnType = elseTransformer.getType();
      } else if (branchIndexForEachRow[i] < whenThenTransformers.size()) {
        resultColumn = thenColumnList.get(branchIndexForEachRow[i]);
        thenColumnType = whenThenTransformers.get(branchIndexForEachRow[i]).right.getType();
      }

      if (resultColumn == null || resultColumn.isNull(i)) {
        builder.appendNull();
      } else {
        writeToColumnBuilder(thenColumnType, resultColumn, i, builder);
      }
    }

    initializeColumnCache(builder.build());
    for (Pair<ColumnTransformer, ColumnTransformer> whenThenColumnTransformer :
        whenThenTransformers) {
      whenThenColumnTransformer.left.clearCache();
      whenThenColumnTransformer.right.clearCache();
      elseTransformer.clearCache();
    }
  }

  protected abstract void writeToColumnBuilder(
      Type thenColumnType, Column column, int index, ColumnBuilder builder);

  @Override
  protected void checkType() {
    // do nothing
  }
}

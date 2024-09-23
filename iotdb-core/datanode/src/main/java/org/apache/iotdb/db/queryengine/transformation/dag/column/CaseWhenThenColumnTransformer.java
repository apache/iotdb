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

package org.apache.iotdb.db.queryengine.transformation.dag.column;

import org.apache.commons.lang3.Validate;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CaseWhenThenColumnTransformer extends ColumnTransformer {
  List<Pair<ColumnTransformer, ColumnTransformer>> whenThenTransformers;
  ColumnTransformer elseTransformer;

  public CaseWhenThenColumnTransformer(
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
  public void evaluateWithSelection(boolean[] selection) {
    int[] branchIndexForEachRow = new int[selection.length];
    for (int i = 0; i < selection.length; i++) {
      if (selection[i]) {
        branchIndexForEachRow[i] = -1;
      } else {
        branchIndexForEachRow[i] = whenThenTransformers.size();
      }
    }
    doTransform(branchIndexForEachRow);
  }

  private void doTransform(int[] branchIndexForEachRow) {

    List<Column> thenColumnList = new ArrayList<>();
    for (int branchIndex = 0; branchIndex < branchIndexForEachRow.length; branchIndex++) {
      boolean[] selection = new boolean[branchIndexForEachRow.length];

      for (int i = 0; i < branchIndexForEachRow.length; i++) {
        if (branchIndexForEachRow[i] == -1) {
          selection[i] = true;
        }
      }

      whenThenTransformers.get(branchIndex).left.evaluateWithSelection(selection);
      Column whenColumn = whenThenTransformers.get(branchIndex).left.getColumn();

      for (int i = 0; i < selection.length; i++) {
        if (!whenColumn.isNull(i) && whenColumn.getBoolean(i)) {
          branchIndexForEachRow[i] = branchIndex;
        } else {
          selection[i] = false;
        }
      }

      whenThenTransformers.get(branchIndex).right.evaluateWithSelection(selection);
      Column thenColumn = whenThenTransformers.get(branchIndex).right.getColumn();
      thenColumnList.add(thenColumn);
    }
    boolean[] selection = new boolean[branchIndexForEachRow.length];

    for (int i = 0; i < branchIndexForEachRow.length; i++) {
      if (branchIndexForEachRow[i] == -1) {
        selection[i] = true;
      }
    }
    elseTransformer.evaluateWithSelection(selection);

    int positionCount = whenThenTransformers.get(0).left.getColumnCachePositionCount();
    ColumnBuilder builder = returnType.createColumnBuilder(positionCount);
    Column elseColumn = elseTransformer.getColumn();
    for (int i = 0; i < branchIndexForEachRow.length; i++) {
      Column resultColumn = null;
      if (branchIndexForEachRow[i] == -1) {
        resultColumn = elseColumn;
      } else if (branchIndexForEachRow[i] < whenThenTransformers.size()) {
        resultColumn = thenColumnList.get(branchIndexForEachRow[i]);
      }
      if (resultColumn == null || resultColumn.isNull(i)) {
        builder.appendNull();
      } else {
        builder.write(resultColumn, i);
      }
    }
    initializeColumnCache(builder.build());
    // 清缓存
    for (Pair<ColumnTransformer, ColumnTransformer> whenThenColumnTransformer :
        whenThenTransformers) {
      whenThenColumnTransformer.left.clearCache();
      whenThenColumnTransformer.right.clearCache();
    }
  }

  @Override
  public void evaluate() {
    int[] branchIndexForEachRow =
        new int[whenThenTransformers.get(0).left.getColumnCachePositionCount()];
    Arrays.fill(branchIndexForEachRow, -1);

    doTransform(branchIndexForEachRow);
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  @Override
  public void clearCache() {
    super.clearCache();
  }
}

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    int[] branchIndexForEachRow = new int[elseTransformer.getColumnCachePositionCount()];
    Arrays.fill(branchIndexForEachRow, -1);

    doTransform(branchIndexForEachRow);
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

  private void doTransform(int[] branch) {
    int[] branchIndexForEachRow = null;
    List<Column> thenColumnList = new ArrayList<>();

    // when and then columns
    for (int i = 0; i < whenThenTransformers.size(); i++) {
      ColumnTransformer whenColumnTransformer = whenThenTransformers.get(i).left;
      whenColumnTransformer.tryEvaluate();
      Column whenColumn = whenColumnTransformer.getColumn();

      int positionCount = whenColumn.getPositionCount();
      boolean[] selection = new boolean[positionCount];

      if (branchIndexForEachRow == null) {
        // init branchIndexForEachRow if it is null
        branchIndexForEachRow = new int[positionCount];
        Arrays.fill(branchIndexForEachRow, -1);
      } else {
        // update selection with branchIndexForEachRow
        for (int j = 0; j < branchIndexForEachRow.length; j++) {
          if (branchIndexForEachRow[j] != -1) {
            selection[j] = true;
          }
        }
      }

      for (int j = 0; j < positionCount; j++) {
        if (!whenColumn.isNull(j) && whenColumn.getBoolean(j)) {
          branchIndexForEachRow[j] = i;
          selection[j] = true;
        } else {
          selection[j] = false;
        }
      }

      ColumnTransformer thenColumnTransformer = whenThenTransformers.get(i).right;
      thenColumnTransformer.evaluateWithSelection(selection);
      Column thenColumn = thenColumnTransformer.getColumn();
      thenColumnList.add(thenColumn);
    }

    // elseColumn
    if (branchIndexForEachRow != null) {
      int positionCount = branchIndexForEachRow.length;
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
        if (branchIndexForEachRow[i] == -1) {
          resultColumn = elseColumn;
        } else if (branchIndexForEachRow[i] < whenThenTransformers.size()) {
          resultColumn = thenColumnList.get(branchIndexForEachRow[i]);
        }

        if (resultColumn == null || resultColumn.isNull(i)) {
          builder.appendNull();
        } else {
          writeToColumnBuilder(
              branchIndexForEachRow[i] == -1
                  ? elseTransformer.getType()
                  : whenThenTransformers.get(branchIndexForEachRow[i]).right.getType(),
              resultColumn,
              i,
              builder);
        }
      }

      initializeColumnCache(builder.build());
      for (Pair<ColumnTransformer, ColumnTransformer> whenThenColumnTransformer :
          whenThenTransformers) {
        whenThenColumnTransformer.left.clearCache();
        whenThenColumnTransformer.right.clearCache();
      }
    }
  }

  protected abstract void writeToColumnBuilder(
      Type thenColumnType, Column column, int index, ColumnBuilder builder);

  @Override
  protected void checkType() {
    // do nothing
  }
}

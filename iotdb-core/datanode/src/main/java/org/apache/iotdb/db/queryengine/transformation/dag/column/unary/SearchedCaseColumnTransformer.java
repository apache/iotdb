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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.NullColumnTransformer;

import org.apache.commons.lang3.Validate;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.AbstractIntType;
import org.apache.tsfile.read.common.type.AbstractLongType;
import org.apache.tsfile.read.common.type.AbstractVarcharType;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class SearchedCaseColumnTransformer extends ColumnTransformer {
  List<Pair<ColumnTransformer, ColumnTransformer>> whenThenTransformers;
  ColumnTransformer elseTransformer;

  public SearchedCaseColumnTransformer(
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

  public SearchedCaseColumnTransformer(
      Type returnType,
      List<ColumnTransformer> whenTransformers,
      List<ColumnTransformer> thenTransformers) {
    super(returnType);
    Validate.isTrue(
        whenTransformers.size() == thenTransformers.size(),
        "the size between whenTransformers and thenTransformers needs to be same");
    this.whenThenTransformers = new ArrayList<>();
    for (int i = 0; i < whenTransformers.size(); i++) {
      this.whenThenTransformers.add(new Pair<>(whenTransformers.get(i), thenTransformers.get(i)));
    }
    this.elseTransformer = new NullColumnTransformer();
  }

  public List<Pair<ColumnTransformer, ColumnTransformer>> getWhenThenColumnTransformers() {
    return whenThenTransformers;
  }

  public ColumnTransformer getElseTransformer() {
    return elseTransformer;
  }

  private void writeToColumnBuilder(
      ColumnTransformer transformer, Column column, int index, ColumnBuilder builder) {
    if (returnType instanceof BooleanType) {
      builder.writeBoolean(transformer.getType().getBoolean(column, index));
    } else if (returnType instanceof AbstractIntType) {
      builder.writeInt(transformer.getType().getInt(column, index));
    } else if (returnType instanceof AbstractLongType) {
      builder.writeLong(transformer.getType().getLong(column, index));
    } else if (returnType instanceof FloatType) {
      builder.writeFloat(transformer.getType().getFloat(column, index));
    } else if (returnType instanceof DoubleType) {
      builder.writeDouble(transformer.getType().getDouble(column, index));
    } else if (returnType instanceof AbstractVarcharType || returnType instanceof BlobType) {
      builder.writeBinary(transformer.getType().getBinary(column, index));
    } else {
      throw new UnsupportedOperationException("Unsupported Type");
    }
  }

  @Override
  protected void evaluate() {
    List<Column> whenColumnList = new ArrayList<>();
    List<Column> thenColumnList = new ArrayList<>();
    for (Pair<ColumnTransformer, ColumnTransformer> whenThenTransformer : whenThenTransformers) {
      whenThenTransformer.left.tryEvaluate();
      whenThenTransformer.right.tryEvaluate();
    }
    if (!(elseTransformer instanceof NullColumnTransformer)) {
      elseTransformer.tryEvaluate();
    }

    int positionCount = whenThenTransformers.get(0).left.getColumnCachePositionCount();
    for (Pair<ColumnTransformer, ColumnTransformer> whenThenColumnTransformer :
        whenThenTransformers) {
      whenColumnList.add(whenThenColumnTransformer.left.getColumn());
      thenColumnList.add(whenThenColumnTransformer.right.getColumn());
    }
    ColumnBuilder builder = returnType.createColumnBuilder(positionCount);

    Column elseColumn =
        elseTransformer instanceof NullColumnTransformer ? null : elseTransformer.getColumn();

    for (int i = 0; i < positionCount; i++) {
      boolean hasValue = false;
      for (int j = 0; j < whenThenTransformers.size(); j++) {
        Column whenColumn = whenColumnList.get(j);
        Column thenColumn = thenColumnList.get(j);
        if (!whenColumn.isNull(i) && whenColumn.getBoolean(i)) {
          if (thenColumn.isNull(i)) {
            builder.appendNull();
          } else {
            writeToColumnBuilder(whenThenTransformers.get(j).right, thenColumn, i, builder);
          }
          hasValue = true;
          break;
        }
      }
      if (!hasValue) {
        if (elseColumn == null) {
          builder.appendNull();
        } else {
          if (!elseColumn.isNull(i)) {
            writeToColumnBuilder(elseTransformer, elseColumn, i, builder);
          } else {
            builder.appendNull();
          }
        }
      }
    }

    initializeColumnCache(builder.build());
  }

  @Override
  protected void checkType() {
    // do nothing
  }
}

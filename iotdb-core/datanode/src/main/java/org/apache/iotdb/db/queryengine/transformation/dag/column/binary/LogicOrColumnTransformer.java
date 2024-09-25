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

package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

public class LogicOrColumnTransformer extends LogicBinaryColumnTransformer {
  public LogicOrColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  @Override
  public void evaluateWithSelection(boolean[] selection) {
    boolean[] selectionCopy = new boolean[selection.length];
    System.arraycopy(selection, 0, selectionCopy, 0, selection.length);

    leftTransformer.evaluateWithSelection(selectionCopy);
    Column leftColumn = leftTransformer.getColumn();
    int positionCount = leftTransformer.getColumnCachePositionCount();

    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && leftColumn.getBoolean(i)) {
        selectionCopy[i] = false;
      }
    }

    rightTransformer.evaluateWithSelection(selectionCopy);
    Column rightColumn = rightTransformer.getColumn();

    ColumnBuilder builder = returnType.createColumnBuilder(positionCount);

    for (int i = 0; i < positionCount; i++) {
      if (selection[i]) {
        if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
          // if both values were not null,then left value must be false
          builder.write(rightColumn, i);
        } else if (!leftColumn.isNull(i)) {
          // right value is null
          if (leftColumn.getBoolean(i)) {
            builder.write(leftColumn, i);
          } else {
            builder.appendNull();
          }
        } else if (!rightColumn.isNull(i)) {
          // left value is null
          if (rightColumn.getBoolean(i)) {
            builder.write(rightColumn, i);
          } else {
            builder.appendNull();
          }
        } else {
          builder.appendNull();
        }
      } else {
        builder.appendNull();
      }
    }

    initializeColumnCache(builder.build());
    this.leftTransformer.clearCache();
    this.rightTransformer.clearCache();
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        returnType.writeBoolean(
            builder,
            transform(
                leftTransformer.getType().getBoolean(leftColumn, i),
                rightTransformer.getType().getBoolean(rightColumn, i)));
      } else if (!leftColumn.isNull(i)) {
        if (leftTransformer.getType().getBoolean(leftColumn, i)) {
          returnType.writeBoolean(builder, true);
        } else {
          builder.appendNull();
        }
      } else if (!rightColumn.isNull(i)) {
        if (rightTransformer.getType().getBoolean(rightColumn, i)) {
          returnType.writeBoolean(builder, true);
        } else {
          builder.appendNull();
        }
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn,
      Column rightColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection) {}

  @Override
  protected boolean transform(boolean left, boolean right) {
    return left || right;
  }
}

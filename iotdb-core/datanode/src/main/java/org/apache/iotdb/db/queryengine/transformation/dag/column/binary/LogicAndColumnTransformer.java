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

public class LogicAndColumnTransformer extends LogicBinaryColumnTransformer {
  public LogicAndColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  @Override
  public void evaluateWithSelection(boolean[] selection) {
    boolean[] selectionCopy = selection.clone();

    leftTransformer.evaluateWithSelection(selectionCopy);
    int positionCount = leftTransformer.getColumnCachePositionCount();
    Column leftColumn = leftTransformer.getColumn();

    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !leftColumn.getBoolean(i)) {
        selectionCopy[i] = false;
      }
    }

    rightTransformer.evaluateWithSelection(selectionCopy);
    Column rightColumn = rightTransformer.getColumn();

    ColumnBuilder builder = returnType.createColumnBuilder(positionCount);
    for (int i = 0; i < positionCount; i++) {
      if (selection[i]) {
        if (selectionCopy[i]) {
          if (rightColumn.isNull(i)) {
            // right is null, whether left is null or left is true, result is null
            builder.appendNull();
          } else if (!rightColumn.getBoolean(i)) {
            // right is false, whether left is null or left is true, result is false
            returnType.writeBoolean(builder, false);
          } else {
            // right is true
            // if left is null, result is null
            // if left is true, result is true
            if (leftColumn.isNull(i)) {
              builder.appendNull();
            } else {
              returnType.writeBoolean(builder, true);
            }
          }
        } else {
          // left value is false
          returnType.writeBoolean(builder, false);
        }
      } else {
        builder.appendNull();
      }
    }

    initializeColumnCache(rightColumn);
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
          builder.appendNull();
        } else {
          returnType.writeBoolean(builder, false);
        }
      } else if (!rightColumn.isNull(i)) {
        if (rightTransformer.getType().getBoolean(rightColumn, i)) {
          builder.appendNull();
        } else {
          returnType.writeBoolean(builder, false);
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
    return left && right;
  }
}

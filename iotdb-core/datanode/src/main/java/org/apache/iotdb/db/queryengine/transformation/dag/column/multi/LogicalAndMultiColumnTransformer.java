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

package org.apache.iotdb.db.queryengine.transformation.dag.column.multi;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.List;

public class LogicalAndMultiColumnTransformer extends LogicalMultiColumnTransformer {
  public LogicalAndMultiColumnTransformer(
      Type returnType, List<ColumnTransformer> columnTransformerList) {
    super(returnType, columnTransformerList);
  }

  @Override
  public void evaluateWithSelection(boolean[] selection) {
    boolean[] selectionCopy = selection.clone();
    boolean[] hasNull = new boolean[selection.length];

    List<Column> childrenColumns = new ArrayList<>();
    for (ColumnTransformer child : columnTransformerList) {
      child.evaluateWithSelection(selectionCopy);
      Column childColumn = child.getColumn();
      childrenColumns.add(childColumn);
      for (int i = 0; i < childColumn.getPositionCount(); i++) {
        if (childColumn.isNull(i)) {
          hasNull[i] = true;
        } else if (!childColumn.getBoolean(i)) {
          // set selection to false if the value is false
          selectionCopy[i] = false;
        }
      }
    }

    int positionCount = childrenColumns.get(0).getPositionCount();
    ColumnBuilder builder = returnType.createColumnBuilder(positionCount);
    for (int i = 0; i < positionCount; i++) {
      if (selection[i]) {
        // have no null, all is true, result will be true
        // have no null, and also have false, result will be false
        // have null, and others are all true, result will be null
        // have null, and also have false, result will be false
        if (!selectionCopy[i]) {
          returnType.writeBoolean(builder, false);
        } else {
          if (hasNull[i]) {
            builder.appendNull();
          } else {
            returnType.writeBoolean(builder, true);
          }
        }
      } else {
        builder.appendNull();
      }
    }

    initializeColumnCache(builder.build());

    // clear children's cache
    for (ColumnTransformer child : columnTransformerList) {
      child.clearCache();
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      boolean result = true;
      boolean hasNull = false;
      for (Column column : childrenColumns) {
        if (column.isNull(i)) {
          hasNull = true;
        } else if (!column.getBoolean(i)) {
          result = false;
          break;
        }
      }

      // have no null, all is true, result will be true
      // have any false, result will be false
      // have null, and others are all true, result will be null
      if (!result) {
        returnType.writeBoolean(builder, false);
      } else if (hasNull) {
        builder.appendNull();
      } else {
        returnType.writeBoolean(builder, true);
      }
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount, boolean[] selection) {
    // do nothing
    throw new UnsupportedOperationException(
        "LogicalAndMultiColumnTransformer do not support doTransform with selection");
  }
}

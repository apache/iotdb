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
import org.apache.tsfile.read.common.type.Type;

import java.util.List;
import java.util.Set;

public class InBooleanMultiColumnTransformer extends InMultiColumnTransformer {

  private final Set<Boolean> constantSet;

  public InBooleanMultiColumnTransformer(
      Set<Boolean> constantSet, List<ColumnTransformer> columnTransformerList) {
    super(columnTransformerList);
    this.constantSet = constantSet;
  }

  @Override
  protected boolean satisfy(List<Column> childrenColumns, int index) {
    boolean value = childrenColumns.get(0).getBoolean(index);
    if (constantSet.contains(value)) {
      return true;
    } else {
      for (int i = 1, size = childrenColumns.size(); i < size; i++) {
        Column valueColumn = childrenColumns.get(i);
        if (!valueColumn.isNull(i)) {
          Type valueType = columnTransformerList.get(i).getType();
          if (valueType.getBoolean(valueColumn, index) == value) {
            return true;
          }
        }
      }
      return false;
    }
  }
}

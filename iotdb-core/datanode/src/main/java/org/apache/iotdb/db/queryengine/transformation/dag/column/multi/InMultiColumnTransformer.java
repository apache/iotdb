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
import org.apache.tsfile.read.common.type.BooleanType;

import java.util.List;

public abstract class InMultiColumnTransformer extends MultiColumnTransformer {

  protected InMultiColumnTransformer(List<ColumnTransformer> columnTransformerList) {
    super(BooleanType.BOOLEAN, columnTransformerList);
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!childrenColumns.get(0).isNull(i)) {
        builder.writeBoolean(satisfy(childrenColumns, i));
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount, boolean[] selection) {
    for (int i = 0; i < positionCount; i++) {
      if (selection[i] && !childrenColumns.get(0).isNull(i)) {
        builder.writeBoolean(satisfy(childrenColumns, i));
      } else {
        builder.appendNull();
      }
    }
  }

  protected abstract boolean satisfy(List<Column> childrenColumns, int index);
}

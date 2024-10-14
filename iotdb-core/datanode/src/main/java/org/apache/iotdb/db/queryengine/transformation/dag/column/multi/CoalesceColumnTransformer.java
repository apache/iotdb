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

import java.util.List;

public class CoalesceColumnTransformer extends MultiColumnTransformer {

  public CoalesceColumnTransformer(Type returnType, List<ColumnTransformer> columnTransformerList) {
    super(returnType, columnTransformerList);
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      transform(childrenColumns, builder, i);
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount, boolean[] selection) {
    for (int i = 0; i < positionCount; i++) {
      if (selection[i]) {
        transform(childrenColumns, builder, i);
      } else {
        builder.appendNull();
      }
    }
  }

  private void transform(List<Column> childrenColumns, ColumnBuilder builder, int i) {
    boolean allNull = true;
    for (Column column : childrenColumns) {
      if (!column.isNull(i)) {
        allNull = false;
        builder.write(column, i);
        break;
      }
    }
    if (allNull) {
      builder.appendNull();
    }
  }

  @Override
  protected void checkType() {
    // do nothing, has checked in FE
  }
}

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
import java.util.stream.Collectors;

public abstract class MultiColumnTransformer extends ColumnTransformer {

  protected final List<ColumnTransformer> columnTransformerList;

  protected MultiColumnTransformer(Type returnType, List<ColumnTransformer> columnTransformerList) {
    super(returnType);
    this.columnTransformerList = columnTransformerList;
    checkType();
  }

  @Override
  public void evaluate() {

    for (ColumnTransformer child : columnTransformerList) {
      child.tryEvaluate();
    }

    // attention: get positionCount before calling getColumn
    int positionCount = columnTransformerList.get(0).getColumnCachePositionCount();

    ColumnBuilder builder = returnType.createColumnBuilder(positionCount);
    doTransform(
        columnTransformerList.stream()
            .map(ColumnTransformer::getColumn)
            .collect(Collectors.toList()),
        builder,
        positionCount);
    initializeColumnCache(builder.build());
  }

  protected abstract void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount);

  public List<ColumnTransformer> getChildren() {
    return columnTransformerList;
  }
}

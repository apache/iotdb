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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

public abstract class UnaryColumnTransformer extends ColumnTransformer {
  protected ColumnTransformer childColumnTransformer;

  protected UnaryColumnTransformer(Type returnType, ColumnTransformer childColumnTransformer) {
    super(returnType);
    this.childColumnTransformer = childColumnTransformer;
    checkType();
  }

  @Override
  public void evaluate() {
    childColumnTransformer.tryEvaluate();
    Column column = childColumnTransformer.getColumn();
    ColumnBuilder columnBuilder = returnType.createColumnBuilder(column.getPositionCount());
    doTransform(column, columnBuilder);
    initializeColumnCache(columnBuilder.build());
  }

  @Override
  public void evaluateWithSelection(boolean[] selection) {
    childColumnTransformer.evaluateWithSelection(selection);
    Column column = childColumnTransformer.getColumn();
    ColumnBuilder columnBuilder = returnType.createColumnBuilder(column.getPositionCount());
    doTransform(column, columnBuilder, selection);
    initializeColumnCache(columnBuilder.build());
    childColumnTransformer.clearCache();
  }

  public ColumnTransformer getChildColumnTransformer() {
    return childColumnTransformer;
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  protected abstract void doTransform(Column column, ColumnBuilder columnBuilder);

  protected abstract void doTransform(
      Column column, ColumnBuilder columnBuilder, boolean[] selection);

  @Override
  public void clearCache() {
    super.clearCache();
    childColumnTransformer.clearCache();
  }
}

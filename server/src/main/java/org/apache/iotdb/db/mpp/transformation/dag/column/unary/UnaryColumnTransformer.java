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

package org.apache.iotdb.db.mpp.transformation.dag.column.unary;

import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;

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

  public ColumnTransformer getChildColumnTransformer() {
    return childColumnTransformer;
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  protected abstract void doTransform(Column column, ColumnBuilder columnBuilder);
}

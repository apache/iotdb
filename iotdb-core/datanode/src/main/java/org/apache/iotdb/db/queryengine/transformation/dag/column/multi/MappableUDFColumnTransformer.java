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
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.access.Column;
import org.apache.iotdb.tsfile.access.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;

public class MappableUDFColumnTransformer extends ColumnTransformer {

  private final ColumnTransformer[] inputColumnTransformers;

  private final UDTFExecutor executor;

  public MappableUDFColumnTransformer(
      Type returnType, ColumnTransformer[] inputColumnTransformers, UDTFExecutor executor) {
    super(returnType);
    this.inputColumnTransformers = inputColumnTransformers;
    this.executor = executor;
  }

  @Override
  public void evaluate() {
    // pull columns from previous transformers
    for (ColumnTransformer inputColumnTransformer : inputColumnTransformers) {
      inputColumnTransformer.tryEvaluate();
    }
    // construct input TsBlock with columns
    int size = inputColumnTransformers.length;
    Column[] columns = new Column[size];
    for (int i = 0; i < size; i++) {
      columns[i] = inputColumnTransformers[i].getColumn();
    }
    // construct TsBlockBuilder
    int count = inputColumnTransformers[0].getColumnCachePositionCount();
    ColumnBuilder builder = returnType.createColumnBuilder(count);
    // executor UDF and cache result
    executor.execute(columns, builder);
    initializeColumnCache(builder.build());
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  public ColumnTransformer[] getInputColumnTransformers() {
    return inputColumnTransformers;
  }

  @Override
  public void close() {
    // finalize executor
    executor.beforeDestroy();
  }
}

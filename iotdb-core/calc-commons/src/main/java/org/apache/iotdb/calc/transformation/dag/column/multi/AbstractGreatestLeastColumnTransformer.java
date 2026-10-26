/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.calc.transformation.dag.column.multi;

import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;

public abstract class AbstractGreatestLeastColumnTransformer extends MultiColumnTransformer {

  protected AbstractGreatestLeastColumnTransformer(
      Type returnType, List<ColumnTransformer> columnTransformerList) {
    super(returnType, columnTransformerList);
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      transform(builder, childrenColumns, i);
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount, boolean[] selection) {
    for (int i = 0; i < positionCount; i++) {
      if (selection[i]) {
        transform(builder, childrenColumns, i);
      } else {
        builder.appendNull();
      }
    }
  }

  protected abstract void transform(ColumnBuilder builder, List<Column> childrenColumns, int index);

  public static ColumnTransformer getGreatestColumnTransformer(
      Type type, List<ColumnTransformer> columnTransformers) {
    return TypeServices.GREATEST_COLUMN_TRANSFORMER_SERVICE.call(type).apply(columnTransformers);
  }

  public static ColumnTransformer getLeastColumnTransformer(
      Type type, List<ColumnTransformer> columnTransformers) {
    return TypeServices.LEAST_COLUMN_TRANSFORMER_SERVICE.call(type).apply(columnTransformers);
  }

  @Override
  protected void checkType() {
    // do nothing
  }
}

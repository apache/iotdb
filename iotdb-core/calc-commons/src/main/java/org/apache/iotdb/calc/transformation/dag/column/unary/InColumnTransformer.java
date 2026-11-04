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

package org.apache.iotdb.calc.transformation.dag.column.unary;

import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.util.Set;

public class InColumnTransformer extends UnaryColumnTransformer {
  private final TypeServices.InColumnValueMatcher valueMatcher;

  public InColumnTransformer(
      Type returnType,
      ColumnTransformer childColumnTransformer,
      boolean isNotIn,
      Set<String> values) {
    super(returnType, childColumnTransformer);
    Type childType = childColumnTransformer.getType();
    if (childType == null) {
      valueMatcher = (column, position) -> false;
      return;
    }
    TypeServices.InColumnValueMatcher matcher =
        TypeServices.IN_COLUMN_VALUE_MATCHER_SERVICE.call(childType).create(values);
    this.valueMatcher =
        isNotIn ? (column, position) -> !matcher.matches(column, position) : matcher;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        transform(column, columnBuilder, i);
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (selection[i] && !column.isNull(i)) {
        transform(column, columnBuilder, i);
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  private void transform(Column column, ColumnBuilder columnBuilder, int i) {
    returnType.writeBoolean(columnBuilder, valueMatcher.matches(column, i));
  }
}

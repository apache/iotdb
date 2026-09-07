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

package org.apache.iotdb.calc.transformation.dag.column.unary.scalar;

import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.iotdb.calc.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.time.ZoneId;

public abstract class AbstractCastFunctionColumnTransformer extends UnaryColumnTransformer {

  private final ZoneId zoneId;
  private final TypeServices.CastInputService castInputService;
  private final TypeServices.CastValueService castValueService;

  protected AbstractCastFunctionColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, ZoneId zoneId) {
    super(returnType, childColumnTransformer);
    this.zoneId = zoneId;
    this.castInputService = TypeServices.CAST_INPUT_SERVICE.call(childColumnTransformer.getType());
    this.castValueService = TypeServices.CAST_VALUE_SERVICE.call(returnType);
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

  protected abstract void transform(Column column, ColumnBuilder columnBuilder, int i);

  protected void castSourceValue(Column column, ColumnBuilder columnBuilder, int position) {
    castInputService.cast(column, columnBuilder, position, castValueService, zoneId);
  }
}

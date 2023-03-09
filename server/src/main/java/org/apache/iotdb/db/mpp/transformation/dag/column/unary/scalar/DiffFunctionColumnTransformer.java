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

package org.apache.iotdb.db.mpp.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;

public class DiffFunctionColumnTransformer extends UnaryColumnTransformer {

  // default is true
  private final boolean ignoreNull;

  // cache the last non-null value
  private double lastValue;

  // indicate whether lastValue is null
  private boolean lastValueIsNull = true;

  public DiffFunctionColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, boolean ignoreNull) {
    super(returnType, childColumnTransformer);
    this.ignoreNull = ignoreNull;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (column.isNull(i)) {
        columnBuilder.appendNull(); // currValue is null, append null

        // When currValue is null:
        // ignoreNull = true, keep lastValueIsNull as before
        // ignoreNull = false, update lastValueIsNull to true
        lastValueIsNull |= !ignoreNull;
      } else {
        double currValue = childColumnTransformer.getType().getDouble(column, i);
        if (lastValueIsNull) {
          columnBuilder.appendNull(); // lastValue is null, append null
        } else {
          returnType.writeDouble(columnBuilder, currValue - lastValue);
        }

        lastValue = currValue; // currValue is not null, update lastValue
        lastValueIsNull = false;
      }
    }
  }
}

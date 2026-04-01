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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.util.BitwiseUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

public class BitwiseOrColumnTransformer extends AbstractBitwiseColumnTransformer {

  public BitwiseOrColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, long rightValue) {
    super(returnType, childColumnTransformer, rightValue);
  }

  @Override
  protected void transform(Column column, ColumnBuilder columnBuilder, int i) {
    long leftValue = column.getLong(i);
    long currentValue = BitwiseUtils.bitwiseOrTransform(leftValue, rightValue);
    columnBuilder.writeLong(currentValue);
  }
}

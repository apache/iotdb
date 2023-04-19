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
import org.apache.iotdb.tsfile.utils.Binary;

public class ReplaceFunctionColumnTransformer extends UnaryColumnTransformer {
  private final String from;
  private final String to;

  public ReplaceFunctionColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, String from, String to) {
    super(returnType, childColumnTransformer);
    this.from = from;
    this.to = to;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        returnType.writeBinary(
            columnBuilder,
            Binary.valueOf(
                childColumnTransformer
                    .getType()
                    .getBinary(column, i)
                    .getStringValue()
                    .replace(from, to)));
      } else {
        columnBuilder.appendNull();
      }
    }
  }
}

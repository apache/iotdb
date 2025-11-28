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
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

public class BlobLengthColumnTransformer extends UnaryColumnTransformer {

  public BlobLengthColumnTransformer(Type returnType, ColumnTransformer childColumnTransformer) {
    super(returnType, childColumnTransformer);
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    doTransform(column, columnBuilder, null);
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {

    int positionCount = column.getPositionCount();
    for (int i = 0; i < positionCount; i++) {
      if ((selection != null && !selection[i]) || column.isNull(i)) {
        columnBuilder.appendNull();
        continue;
      }

      Binary value = column.getBinary(i);
      int length = value.getValues().length;
      columnBuilder.writeInt(length);
    }
  }
}

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

public class StartsWithColumnTransformer extends UnaryColumnTransformer {
  private final byte[] prefix;

  public StartsWithColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, String prefixStr) {
    super(returnType, childColumnTransformer);
    this.prefix = prefixStr.getBytes();
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        byte[] currentValue = column.getBinary(i).getValues();
        columnBuilder.writeBoolean(equalCompare(currentValue, prefix, 0));
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  public static boolean equalCompare(byte[] value, byte[] pattern, int offset) {
    if (value.length < pattern.length) {
      return false;
    }
    for (int i = 0; (i < pattern.length) && (i + offset < value.length); i++) {
      if (value[i + offset] != pattern[i]) {
        return false;
      }
    }
    return true;
  }
}

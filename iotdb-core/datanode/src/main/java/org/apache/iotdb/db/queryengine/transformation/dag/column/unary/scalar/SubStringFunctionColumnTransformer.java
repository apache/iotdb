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
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.utils.Binary;

public class SubStringFunctionColumnTransformer extends UnaryColumnTransformer {

  private int beginPosition;
  private int endPosition;
  public static final String EMPTY_STRING = "";

  public SubStringFunctionColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, int beginPosition, int length) {
    super(returnType, childColumnTransformer);
    this.endPosition =
        (length == Integer.MAX_VALUE ? Integer.MAX_VALUE : beginPosition + length - 1);
    this.beginPosition = beginPosition > 0 ? beginPosition - 1 : 0;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        String currentValue = column.getBinary(i).getStringValue();
        if (beginPosition >= currentValue.length() || endPosition < 0) {
          currentValue = EMPTY_STRING;
        } else {
          if (endPosition >= currentValue.length()) {
            currentValue = currentValue.substring(beginPosition);
          } else {
            currentValue = currentValue.substring(beginPosition, endPosition);
          }
        }
        columnBuilder.writeBinary(Binary.valueOf(currentValue));
      } else {
        columnBuilder.appendNull();
      }
    }
  }
}

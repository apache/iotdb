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
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;
import org.apache.iotdb.tsfile.utils.Binary;

public class SubStrFunctionColumnTransformer extends UnaryColumnTransformer {

  protected int beginPosition;
  protected int endPosition;

  public SubStrFunctionColumnTransformer(
      Type returnType,
      ColumnTransformer childColumnTransformer,
      int beginPosition,
      int endPosition) {
    super(returnType, childColumnTransformer);
    this.beginPosition = beginPosition;
    this.endPosition = endPosition;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    TypeEnum sourceType = childColumnTransformer.getType().getTypeEnum();
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        if (sourceType == TypeEnum.BINARY) {
          String currentValue = column.getBinary(i).getStringValue();
          if (endPosition == 0) {
            columnBuilder.writeBinary(Binary.valueOf(currentValue.substring(beginPosition)));
          } else {
            columnBuilder.writeBinary(
                Binary.valueOf(currentValue.substring(beginPosition, endPosition)));
          }
        } else {
          throw new UnsupportedOperationException(
              String.format(
                  "Unsupported source dataType: %s",
                  childColumnTransformer.getType().getTypeEnum()));
        }
      } else {
        columnBuilder.appendNull();
      }
    }
  }
}

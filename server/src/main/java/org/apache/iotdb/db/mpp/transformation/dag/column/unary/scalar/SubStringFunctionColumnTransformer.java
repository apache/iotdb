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

public class SubStringFunctionColumnTransformer extends UnaryColumnTransformer {

  private int beginPosition;
  private int endPosition;
  private int length;

  private boolean hasLength;

  public static final String EMPTY_STRING = "";

  public SubStringFunctionColumnTransformer(
      Type returnType,
      ColumnTransformer childColumnTransformer,
      int beginPosition,
      int length,
      boolean hasLength) {
    super(returnType, childColumnTransformer);
    this.beginPosition = beginPosition - 1;
    this.endPosition = beginPosition + length - 1;
    this.length = length;
    this.hasLength = hasLength;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    TypeEnum sourceType = childColumnTransformer.getType().getTypeEnum();
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        if (sourceType == TypeEnum.BINARY) {
          String currentValue = column.getBinary(i).getStringValue();
          if (!hasLength) {
            if (beginPosition >= currentValue.length()) {
              currentValue = EMPTY_STRING;
            } else if (beginPosition >= 0) {
              currentValue = currentValue.substring(beginPosition);
            }
          } else {
            if (length < 0) {
              throw new UnsupportedOperationException(
                  "Argument exception,the scalar function [SUBSTRING] substring length has to be greater than 0");
            }
            if (beginPosition < 0) {
              beginPosition = 0;
              if (endPosition >= currentValue.length()) {
                currentValue = currentValue.substring(beginPosition);
              } else if (endPosition < 0) {
                currentValue = EMPTY_STRING;
              } else {
                currentValue = currentValue.substring(beginPosition, endPosition);
              }
            } else if (beginPosition >= currentValue.length()) {
              currentValue = EMPTY_STRING;
            } else {
              if (endPosition >= currentValue.length()) {
                currentValue = currentValue.substring(beginPosition);
              } else {
                currentValue = currentValue.substring(beginPosition, endPosition);
              }
            }
          }
          columnBuilder.writeBinary(Binary.valueOf(currentValue));
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

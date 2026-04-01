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

package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.strategies.HmacStrategy;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import static org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.util.TransformerDebugUtils.generateOriginalValue;

public class HmacColumnTransformer extends BinaryColumnTransformer {

  private final HmacStrategy hmacStrategy;
  private final String functionName;
  private final Type inputType;

  public HmacColumnTransformer(
      Type returnType,
      ColumnTransformer leftTransformer,
      ColumnTransformer rightTransformer,
      HmacStrategy hmacStrategy,
      String functionName,
      Type inputType) {
    super(returnType, leftTransformer, rightTransformer);
    this.hmacStrategy = hmacStrategy;
    this.functionName = functionName;
    this.inputType = inputType;
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (leftColumn.isNull(i) || rightColumn.isNull(i)) {
        builder.appendNull();
      } else {
        byte[] data = leftColumn.getBinary(i).getValues();
        byte[] key = rightColumn.getBinary(i).getValues();
        byte[] hmacBytes;
        try {
          hmacBytes = hmacStrategy.hmacTransform(data, key);
        } catch (IllegalArgumentException e) {
          String errorMessage =
              String.format(
                  "Failed to execute function '%s' due to an invalid input format. the value '%s' corresponding to a empty key, the empty key is not allowed in HMAC operation.",
                  functionName, generateOriginalValue(data, inputType));
          throw new SemanticException(errorMessage);
        }
        builder.writeBinary(new Binary(hmacBytes));
      }
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn,
      Column rightColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection) {

    for (int i = 0; i < positionCount; i++) {
      if (selection[i] && !leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        byte[] data = leftColumn.getBinary(i).getValues();
        byte[] key = rightColumn.getBinary(i).getValues();
        byte[] hmacBytes;
        try {
          hmacBytes = hmacStrategy.hmacTransform(data, key);

        } catch (IllegalArgumentException e) {
          String errorMessage =
              String.format(
                  "Failed to execute function '%s' due to an invalid input format. the value '%s' corresponding to a empty key, the empty key is not allowed in HMAC operation.",
                  functionName, generateOriginalValue(data, inputType));
          throw new SemanticException(errorMessage);
        }
        builder.writeBinary(new Binary(hmacBytes));
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void checkType() {}
}

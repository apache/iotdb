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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.inteface.CodecStrategy;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import static org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.util.TransformerDebugUtils.generateOriginalValue;

/**
 * A generic, reusable column converter for handling all codec-based encoding/decoding functions. It
 * implements concrete conversion logic by composing a {@link CodecStrategy} strategy object,
 */
public class GenericCodecColumnTransformer extends UnaryColumnTransformer {

  private final CodecStrategy strategy;
  private final String functionName;
  private final Type inputType;

  /**
   * @param strategy specific codec strategy for transformation
   * @param functionName name of the function, used for error message
   */
  public GenericCodecColumnTransformer(
      Type returnType,
      ColumnTransformer childColumnTransformer,
      CodecStrategy strategy,
      String functionName,
      Type inputType) {
    super(returnType, childColumnTransformer);
    this.strategy = strategy;
    this.functionName = functionName;
    this.inputType = inputType;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    doTransform(column, columnBuilder, null);
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (column.isNull(i) || (selection != null && !selection[i])) {
        columnBuilder.appendNull();
        continue;
      }

      byte[] inputBytes = column.getBinary(i).getValues();
      try {
        // use the composed Codec strategy to perform the core
        byte[] outputBytes = strategy.codeCTransform(inputBytes);
        columnBuilder.writeBinary(new Binary(outputBytes));

      } catch (SemanticException e) {

        // The decoding functions may throw IllegalArgumentException when the input is invalid.
        // show the original value in the error message to help users debug.
        String problematicValue = generateOriginalValue(inputBytes, inputType);
        String errorMessage =
            String.format(
                "Failed to execute function '%s' due to an invalid input format. Problematic value: %s",
                functionName, problematicValue);
        throw new SemanticException(errorMessage);
      }
    }
  }
}

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
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory.NumericCodecStrategiesFactory;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import static org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.util.TransformerDebugUtils.generateOriginalValue;

/** A transformer that converts byte array representations to long values */
public class BytesToLongColumnTransformer extends UnaryColumnTransformer {

  private final NumericCodecStrategiesFactory.BytesToLongStrategy bytesToLongStrategy;
  private final String functionName;
  private final Type inputType;

  public BytesToLongColumnTransformer(
      Type returnType,
      ColumnTransformer childColumnTransformer,
      NumericCodecStrategiesFactory.BytesToLongStrategy bytesToLongStrategy,
      String functionName,
      Type inputType) {
    super(returnType, childColumnTransformer);
    this.bytesToLongStrategy = bytesToLongStrategy;
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
        long outputValue = bytesToLongStrategy.numericCodeCTransform(inputBytes);
        columnBuilder.writeLong(outputValue);

      } catch (SemanticException e) {
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

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

package org.apache.iotdb.db.queryengine.plan.expression.multi.builtin;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.transformation.api.LayerPointReader;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.Transformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor.checkFunctionExpressionInputSize;

/**
 * This interface defines the methods that FunctionExpression may use if it is a FunctionExpression
 * representing a built-in function
 */
public interface BuiltInScalarFunctionHelper extends BuiltInFunctionHelper {
  /**
   * Check if the input size is correct. For example, function DIFF only supports one column as
   * input. Throw {@link SemanticException} if the input size is not correct.
   */
  default void checkBuiltInScalarFunctionInputSize(FunctionExpression functionExpression)
      throws SemanticException {
    checkFunctionExpressionInputSize(
        functionExpression.getExpressionString(), functionExpression.getExpressions().size(), 1);
  }

  /**
   * Check if the input TsDataType is correct. Throw {@link SemanticException} if the type is not
   * correct.
   *
   * @param tsDataType TSDataType of input series.
   */
  void checkBuiltInScalarFunctionInputDataType(TSDataType tsDataType) throws SemanticException;

  /**
   * Return the type of data after input is processed by this scalar function.
   *
   * @return TsDataType
   */
  TSDataType getBuiltInScalarFunctionReturnType(FunctionExpression functionExpression);

  /**
   * @param expression The FunctionExpression representing the scalar function
   * @param columnTransformer input ColumnTransformer
   * @return Specific ColumnTransformer of this scalar function
   */
  ColumnTransformer getBuiltInScalarFunctionColumnTransformer(
      FunctionExpression expression, ColumnTransformer columnTransformer);

  /**
   * Construct a {@link Transformer} for this built-in function in {@link
   * org.apache.iotdb.db.queryengine.plan.expression.visitor.IntermediateLayerVisitor}
   *
   * @param expression The FunctionExpression representing the scalar function
   * @param layerPointReader input reader
   * @return Specific Transformer of this scalar function
   */
  Transformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, LayerPointReader layerPointReader);

  /**
   * Some built-in scalar functions may have a different header. This method will be called by
   * {@link FunctionExpression#getExpressionStringInternal()} )}
   *
   * @param builder String builder in FunctionExpression. Append function attributes through it.
   * @param functionAttributes attributes of the function
   */
  default void appendFunctionAttributes(
      boolean hasExpression, StringBuilder builder, Map<String, String> functionAttributes) {
    FunctionExpression.appendAttributes(hasExpression, builder, functionAttributes);
  }
}

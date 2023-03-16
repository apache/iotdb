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

package org.apache.iotdb.db.mpp.plan.expression.multi.builtin.helper;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.multi.builtin.BuiltInScalarFunctionHelper;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.scalar.SubStrFunctionColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.unary.scalar.SubStrFunctionTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.type.TypeFactory;

import java.util.Map;

import static org.apache.iotdb.db.constant.SqlConstant.SUBSTR_END;
import static org.apache.iotdb.db.constant.SqlConstant.SUBSTR_START;
import static org.apache.iotdb.db.mpp.plan.parser.ASTVisitor.checkFunctionExpressionInputSize;

public class SubStrFunctionHelper implements BuiltInScalarFunctionHelper {
  @Override
  public void checkBuiltInScalarFunctionInputSize(FunctionExpression functionExpression)
      throws SemanticException {
    checkFunctionExpressionInputSize(
        functionExpression.getExpressionString(), functionExpression.getExpressions().size(), 1);
  }

  @Override
  public void checkBuiltInScalarFunctionInputDataType(TSDataType tsDataType)
      throws SemanticException {
    if (tsDataType.getType() == TSDataType.TEXT.getType()) {
      return;
    }
    throw new SemanticException(
        "Input series of Scalar function [SUBSTR] only supports numeric data types [TEXT]");
  }

  @Override
  public TSDataType getBuiltInScalarFunctionReturnType(FunctionExpression functionExpression) {
    return TSDataType.TEXT;
  }

  @Override
  public ColumnTransformer getBuiltInScalarFunctionColumnTransformer(
      FunctionExpression expression, ColumnTransformer columnTransformer) {
    return new SubStrFunctionColumnTransformer(
        TypeFactory.getType(this.getBuiltInScalarFunctionReturnType(expression)),
        columnTransformer,
        Integer.parseInt(expression.getFunctionAttributes().getOrDefault(SUBSTR_START, "0")),
        Integer.parseInt(expression.getFunctionAttributes().getOrDefault(SUBSTR_END, "0")));
  }

  @Override
  public Transformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, LayerPointReader layerPointReader) {
    return new SubStrFunctionTransformer(
        layerPointReader,
        Integer.parseInt(expression.getFunctionAttributes().getOrDefault(SUBSTR_START, "0")),
        Integer.parseInt(expression.getFunctionAttributes().getOrDefault(SUBSTR_END, "0")));
  }

  @Override
  public void appendFunctionAttributes(
      boolean hasExpression, StringBuilder builder, Map<String, String> functionAttributes) {
    builder.append(",").append(functionAttributes.get(SUBSTR_START));
    if (functionAttributes.containsKey(SUBSTR_END)) {
      builder.append(",").append(functionAttributes.get(SUBSTR_END));
    }
  }
}

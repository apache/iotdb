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
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.scalar.ReplaceFunctionColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.unary.scalar.ReplaceFunctionTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.type.TypeFactory;

import java.util.Map;

import static org.apache.iotdb.db.constant.SqlConstant.REPLACE_FROM;
import static org.apache.iotdb.db.constant.SqlConstant.REPLACE_TO;

public class ReplaceFunctionHelper implements BuiltInScalarFunctionHelper {
  @Override
  public void checkBuiltInScalarFunctionInputDataType(TSDataType tsDataType)
      throws SemanticException {
    if (tsDataType.equals(TSDataType.TEXT)) {
      return;
    }
    throw new SemanticException(
        "Input series of Scalar function [REPLACE] only support text data type.");
  }

  @Override
  public TSDataType getBuiltInScalarFunctionReturnType(FunctionExpression functionExpression) {
    return TSDataType.TEXT;
  }

  @Override
  public ColumnTransformer getBuiltInScalarFunctionColumnTransformer(
      FunctionExpression expression, ColumnTransformer columnTransformer) {
    checkFromAndToAttributes(expression);
    return new ReplaceFunctionColumnTransformer(
        TypeFactory.getType(TSDataType.TEXT),
        columnTransformer,
        expression.getFunctionAttributes().get(REPLACE_FROM),
        expression.getFunctionAttributes().get(REPLACE_TO));
  }

  @Override
  public Transformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, LayerPointReader layerPointReader) {
    checkFromAndToAttributes(expression);
    return new ReplaceFunctionTransformer(
        layerPointReader,
        expression.getFunctionAttributes().get(REPLACE_FROM),
        expression.getFunctionAttributes().get(REPLACE_TO));
  }

  @Override
  public void appendFunctionAttributes(
      boolean hasExpression, StringBuilder builder, Map<String, String> functionAttributes) {
    builder.append(", '");
    builder.append(functionAttributes.get(REPLACE_FROM));
    builder.append("', '");
    builder.append(functionAttributes.get(REPLACE_TO));
    builder.append("'");
  }

  private void checkFromAndToAttributes(FunctionExpression expression) {
    if (!expression.getFunctionAttributes().containsKey(REPLACE_FROM)
        || !expression.getFunctionAttributes().containsKey(REPLACE_TO)) {
      throw new SemanticException("Function REPLACE must specify from and to component.");
    }
  }
}

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

package org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.helper;

import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.CastFunctionColumnTransformer;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.BuiltInScalarFunctionHelper;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.scalar.CastFunctionTransformer;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.time.ZoneId;
import java.util.Map;

import static org.apache.iotdb.calc.utils.constant.SqlConstant.CAST_TYPE;

public class CastFunctionHelper implements BuiltInScalarFunctionHelper {

  @Override
  public void checkBuiltInScalarFunctionInputDataType(TSDataType tsDataType)
      throws SemanticException {
    // needn't check
  }

  @Override
  public TSDataType getBuiltInScalarFunctionReturnType(FunctionExpression functionExpression) {
    if (!functionExpression.getFunctionAttributes().containsKey(CAST_TYPE)) {
      throw new SemanticException("Function Cast must specify a target data type.");
    }
    return TSDataType.valueOf(
        functionExpression.getFunctionAttributes().get(CAST_TYPE).toUpperCase());
  }

  @Override
  public ColumnTransformer getBuiltInScalarFunctionColumnTransformer(
      FunctionExpression expression, ColumnTransformer columnTransformer) {
    return new CastFunctionColumnTransformer(
        TypeFactory.getType(this.getBuiltInScalarFunctionReturnType(expression)),
        columnTransformer,
        ZoneId.systemDefault());
  }

  @Override
  public Transformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, LayerReader layerReader) {
    return new CastFunctionTransformer(
        layerReader, this.getBuiltInScalarFunctionReturnType(expression));
  }

  @Override
  public void appendFunctionAttributes(
      boolean hasExpression, StringBuilder builder, Map<String, String> functionAttributes) {
    // Cast has only one attribute
    builder.append(" AS ");
    builder.append(functionAttributes.entrySet().iterator().next().getValue());
  }
}

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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.BuiltInScalarFunctionHelper;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.Transformer;

import org.apache.tsfile.enums.TSDataType;

import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor.checkFunctionExpressionInputSize;

public class ReadObjectFunctionHelper implements BuiltInScalarFunctionHelper {
  @Override
  public void checkBuiltInScalarFunctionInputSize(FunctionExpression functionExpression)
      throws SemanticException {
    checkFunctionExpressionInputSize(
        functionExpression.getExpressionString(),
        functionExpression.getExpressions().size(),
        1,
        2,
        3);
  }

  @Override
  public void checkBuiltInScalarFunctionInputDataType(TSDataType tsDataType)
      throws SemanticException {
    if (tsDataType == TSDataType.OBJECT) {
      return;
    }
    throw new SemanticException(
        "Input series of Scalar function [READ_OBJECT] only supports data type [OBJECT]");
  }

  @Override
  public TSDataType getBuiltInScalarFunctionReturnType(FunctionExpression functionExpression) {
    return TSDataType.BLOB;
  }

  @Override
  public ColumnTransformer getBuiltInScalarFunctionColumnTransformer(
      FunctionExpression expression, ColumnTransformer columnTransformer) {
    return null;
  }

  @Override
  public Transformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, LayerReader layerReader) {
    return null;
  }

  @Override
  public void appendFunctionAttributes(
      boolean hasExpression, StringBuilder builder, Map<String, String> functionAttributes) {
    BuiltInScalarFunctionHelper.super.appendFunctionAttributes(
        hasExpression, builder, functionAttributes);
  }
}

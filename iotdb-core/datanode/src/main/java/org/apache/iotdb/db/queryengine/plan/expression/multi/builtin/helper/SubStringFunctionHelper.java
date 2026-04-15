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
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.scalar.SubStringFunctionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.scalar.SubStringFunctionTransformer;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.iotdb.db.utils.constant.SqlConstant.SUBSTRING_FOR;
import static org.apache.iotdb.db.utils.constant.SqlConstant.SUBSTRING_FROM;
import static org.apache.iotdb.db.utils.constant.SqlConstant.SUBSTRING_IS_STANDARD;
import static org.apache.iotdb.db.utils.constant.SqlConstant.SUBSTRING_LENGTH;
import static org.apache.iotdb.db.utils.constant.SqlConstant.SUBSTRING_START;

public class SubStringFunctionHelper implements BuiltInScalarFunctionHelper {
  public static final String BLANK_STRING = " ";
  public static final String COMMA_STRING = ",";

  public static final String NULL_STRING = "null";

  @Override
  public void checkBuiltInScalarFunctionInputSize(FunctionExpression functionExpression)
      throws SemanticException {
    if (functionExpression.getFunctionAttributes().isEmpty()
        || functionExpression.getExpressions().size() != 1) {
      throw new SemanticException(
          "Argument exception,the scalar function [SUBSTRING] needs at least one argument,it must be a signed integer");
    }
    if (functionExpression.getExpressionString().contains(NULL_STRING)) {
      throw new SemanticException(
          "Syntax error,please check that the parameters of the function are correct");
    }
  }

  @Override
  public void checkBuiltInScalarFunctionInputDataType(TSDataType tsDataType)
      throws SemanticException {
    if (TSDataType.TEXT.equals(tsDataType) || TSDataType.STRING.equals(tsDataType)) {
      return;
    }
    throw new SemanticException(
        String.format("Unsupported data type %s for function SUBSTRING.", tsDataType));
  }

  @Override
  public TSDataType getBuiltInScalarFunctionReturnType(FunctionExpression functionExpression) {
    return TSDataType.TEXT;
  }

  @Override
  public ColumnTransformer getBuiltInScalarFunctionColumnTransformer(
      FunctionExpression expression, ColumnTransformer columnTransformer) {
    LinkedHashMap<String, String> functionAttributes = expression.getFunctionAttributes();
    int subStringLength =
        Integer.parseInt(
            functionAttributes.getOrDefault(SUBSTRING_LENGTH, String.valueOf(Integer.MAX_VALUE)));
    int subStringStart = Integer.parseInt(functionAttributes.getOrDefault(SUBSTRING_START, "0"));
    if (subStringLength < 0 || subStringStart < 0) {
      throw new SemanticException(
          "Argument exception,the scalar function [SUBSTRING] beginPosition and length must be greater than 0");
    }

    return new SubStringFunctionColumnTransformer(
        TypeFactory.getType(this.getBuiltInScalarFunctionReturnType(expression)),
        columnTransformer,
        subStringStart,
        subStringLength);
  }

  @Override
  public Transformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, LayerReader layerReader) {
    LinkedHashMap<String, String> functionAttributes = expression.getFunctionAttributes();
    int subStringLength =
        Integer.parseInt(
            functionAttributes.getOrDefault(SUBSTRING_LENGTH, String.valueOf(Integer.MAX_VALUE)));
    int subStringStart = Integer.parseInt(functionAttributes.getOrDefault(SUBSTRING_START, "0"));
    if (subStringLength < 0 || subStringStart < 0) {
      throw new SemanticException(
          "Argument exception,the scalar function [SUBSTRING] beginPosition and length must be greater than 0");
    }
    return new SubStringFunctionTransformer(layerReader, subStringStart, subStringLength);
  }

  @Override
  public void appendFunctionAttributes(
      boolean hasExpression, StringBuilder builder, Map<String, String> functionAttributes) {

    if (functionAttributes.containsKey(SUBSTRING_IS_STANDARD)) {
      builder
          .append(BLANK_STRING)
          .append(SUBSTRING_FROM)
          .append(BLANK_STRING)
          .append(functionAttributes.get(SUBSTRING_START));
      if (functionAttributes.containsKey(SUBSTRING_LENGTH)) {
        builder
            .append(BLANK_STRING)
            .append(SUBSTRING_FOR)
            .append(BLANK_STRING)
            .append(functionAttributes.get(SUBSTRING_LENGTH));
      }
    } else {
      builder.append(COMMA_STRING).append(functionAttributes.get(SUBSTRING_START));
      if (functionAttributes.containsKey(SUBSTRING_LENGTH)) {
        builder.append(COMMA_STRING).append(functionAttributes.get(SUBSTRING_LENGTH));
      }
    }
  }
}

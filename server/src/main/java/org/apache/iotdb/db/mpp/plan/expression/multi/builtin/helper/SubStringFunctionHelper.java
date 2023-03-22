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
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.scalar.SubStringFunctionColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.unary.scalar.SubStringFunctionTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.type.TypeFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.iotdb.db.constant.SqlConstant.SUBSTRING_FOR;
import static org.apache.iotdb.db.constant.SqlConstant.SUBSTRING_FOR_LENGTH;
import static org.apache.iotdb.db.constant.SqlConstant.SUBSTRING_FROM;
import static org.apache.iotdb.db.constant.SqlConstant.SUBSTRING_IS_STANDARD;
import static org.apache.iotdb.db.constant.SqlConstant.SUBSTRING_LENGTH;
import static org.apache.iotdb.db.constant.SqlConstant.SUBSTRING_START;

public class SubStringFunctionHelper implements BuiltInScalarFunctionHelper {
  public static final String BLANK_STRING = " ";
  public static final String COMMA_STRING = ",";

  @Override
  public void checkBuiltInScalarFunctionInputSize(FunctionExpression functionExpression)
      throws SemanticException {
    if (functionExpression.getFunctionAttributes().isEmpty()
        || functionExpression.getExpressions().size() > 1) {
      throw new SemanticException(
          "Argument exception,the scalar function [SUBSTRING] needs at least one argument,it must be a signed integer");
    }
  }

  @Override
  public void checkBuiltInScalarFunctionInputDataType(TSDataType tsDataType)
      throws SemanticException {
    if (tsDataType.getType() == TSDataType.TEXT.getType()) {
      return;
    }
    throw new SemanticException(
        "Input series of Scalar function [SUBSTRING] only supports numeric data types [TEXT]");
  }

  @Override
  public TSDataType getBuiltInScalarFunctionReturnType(FunctionExpression functionExpression) {
    return TSDataType.TEXT;
  }

  @Override
  public ColumnTransformer getBuiltInScalarFunctionColumnTransformer(
      FunctionExpression expression, ColumnTransformer columnTransformer) {
    LinkedHashMap<String, String> functionAttributes = expression.getFunctionAttributes();
    return new SubStringFunctionColumnTransformer(
        TypeFactory.getType(this.getBuiltInScalarFunctionReturnType(expression)),
        columnTransformer,
        Integer.parseInt(
            functionAttributes.containsKey(SUBSTRING_IS_STANDARD)
                ? functionAttributes.getOrDefault(SUBSTRING_FROM, "0")
                : functionAttributes.getOrDefault(SUBSTRING_START, "0")),
        Integer.parseInt(
            functionAttributes.containsKey(SUBSTRING_IS_STANDARD)
                ? functionAttributes.getOrDefault(SUBSTRING_FOR_LENGTH, "0")
                : functionAttributes.getOrDefault(SUBSTRING_LENGTH, "0")),
        functionAttributes.containsKey(SUBSTRING_IS_STANDARD)
            ? functionAttributes.containsKey(SUBSTRING_FOR_LENGTH)
            : functionAttributes.containsKey(SUBSTRING_LENGTH));
  }

  @Override
  public Transformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, LayerPointReader layerPointReader) {
    LinkedHashMap<String, String> functionAttributes = expression.getFunctionAttributes();
    return new SubStringFunctionTransformer(
        layerPointReader,
        Integer.parseInt(
            functionAttributes.containsKey(SUBSTRING_IS_STANDARD)
                ? functionAttributes.getOrDefault(SUBSTRING_FROM, "0")
                : functionAttributes.getOrDefault(SUBSTRING_START, "0")),
        Integer.parseInt(
            functionAttributes.containsKey(SUBSTRING_IS_STANDARD)
                ? functionAttributes.getOrDefault(SUBSTRING_FOR_LENGTH, "0")
                : functionAttributes.getOrDefault(SUBSTRING_LENGTH, "0")),
        functionAttributes.containsKey(SUBSTRING_IS_STANDARD)
            ? functionAttributes.containsKey(SUBSTRING_FOR_LENGTH)
            : functionAttributes.containsKey(SUBSTRING_LENGTH));
  }

  @Override
  public void appendFunctionAttributes(
      boolean hasExpression, StringBuilder builder, Map<String, String> functionAttributes) {

    if (functionAttributes.containsKey(SUBSTRING_IS_STANDARD)) {
      builder
          .append(BLANK_STRING)
          .append(SUBSTRING_FROM)
          .append(BLANK_STRING)
          .append(functionAttributes.get(SUBSTRING_FROM));
      if (functionAttributes.containsKey(SUBSTRING_FOR_LENGTH)) {
        builder
            .append(BLANK_STRING)
            .append(SUBSTRING_FOR)
            .append(BLANK_STRING)
            .append(functionAttributes.get(SUBSTRING_FOR_LENGTH));
      }
    } else {
      builder.append(COMMA_STRING).append(functionAttributes.get(SUBSTRING_START));
      if (functionAttributes.containsKey(SUBSTRING_LENGTH)) {
        builder.append(COMMA_STRING).append(functionAttributes.get(SUBSTRING_LENGTH));
      }
    }
  }
}

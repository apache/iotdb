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
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.multi.builtin.BuiltInScalarFunctionHelper;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.scalar.RoundFunctionColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.unary.scalar.RoundFunctionTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.type.TypeFactory;

import java.util.List;

import static org.apache.iotdb.db.mpp.plan.parser.ASTVisitor.checkFunctionExpressionInputSize;

public class RoundFunctionHelper implements BuiltInScalarFunctionHelper {
  @Override
  public void checkBuiltInScalarFunctionInputSize(FunctionExpression functionExpression)
      throws SemanticException {
    checkFunctionExpressionInputSize(
        functionExpression.getExpressionString(), functionExpression.getExpressions().size(), 1);
  }

  @Override
  public void checkBuiltInScalarFunctionInputDataType(TSDataType tsDataType)
      throws SemanticException {
    if (tsDataType.isNumeric()) {
      return;
    }
    throw new SemanticException(
        "Input series of Scalar function [ROUND] only supports numeric data types [INT32, INT64, FLOAT, DOUBLE]");
  }

  @Override
  public TSDataType getBuiltInScalarFunctionReturnType(FunctionExpression functionExpression) {
    return TSDataType.DOUBLE;
  }

  @Override
  public ColumnTransformer getBuiltInScalarFunctionColumnTransformer(
      FunctionExpression expression, ColumnTransformer columnTransformer) {
    List<Expression> expressions = expression.getExpressions();
    if (expressions.size() == 1) {
      return new RoundFunctionColumnTransformer(
          TypeFactory.getType(this.getBuiltInScalarFunctionReturnType(expression)),
          columnTransformer,
          0);
    }
    return new RoundFunctionColumnTransformer(
        TypeFactory.getType(this.getBuiltInScalarFunctionReturnType(expression)),
        columnTransformer,
        Integer.parseInt(expressions.get(1).getExpressionString()));
  }

  @Override
  public Transformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, LayerPointReader layerPointReader) {
    List<Expression> expressions = expression.getExpressions();
    if (expressions.size() == 1) {
      return new RoundFunctionTransformer(
          layerPointReader, this.getBuiltInScalarFunctionReturnType(expression), 0);
    }
    return new RoundFunctionTransformer(
        layerPointReader,
        this.getBuiltInScalarFunctionReturnType(expression),
        Integer.parseInt(expressions.get(1).getExpressionString()));
  }
}

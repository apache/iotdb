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
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.scalar.CastFunctionColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.unary.scalar.CastFunctionTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.type.TypeFactory;

import static org.apache.iotdb.db.constant.SqlConstant.CAST_TYPE;
import static org.apache.iotdb.db.mpp.plan.parser.ASTVisitor.checkFunctionExpressionInputSize;

public class CastHelper implements BuiltInScalarFunctionHelper {
  @Override
  public void checkBuiltInScalarFunctionInputSize(FunctionExpression functionExpression)
      throws SemanticException {
    checkFunctionExpressionInputSize(
        functionExpression.getExpressionString(), functionExpression.getExpressions().size(), 1);
  }

  @Override
  public void checkBuiltInScalarFunctionInputDataType(TSDataType tsDataType)
      throws SemanticException {}

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
        columnTransformer);
  }

  @Override
  public Transformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, LayerPointReader layerPointReader) {
    return new CastFunctionTransformer(
        layerPointReader, this.getBuiltInScalarFunctionReturnType(expression));
  }

  public static int castLongToInt(long value) {
    if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
      throw new RuntimeException(
          String.format("long value %d is out of range of integer value.", value));
    }
    return (int) value;
  }

  public static int castFloatToInt(float value) {
    if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
      throw new RuntimeException(
          String.format("Float value %f is out of range of integer value.", value));
    }
    return Math.round(value);
  }

  public static long castFloatToLong(float value) {
    if (value > Long.MAX_VALUE || value < Long.MIN_VALUE) {
      throw new RuntimeException(
          String.format("Float value %f is out of range of long value.", value));
    }
    return Math.round((double) value);
  }

  public static int castDoubleToInt(double value) {
    if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
      throw new RuntimeException(
          String.format("Double value %f is out of range of integer value.", value));
    }
    return Math.round((float) value);
  }

  public static long castDoubleToLong(double value) {
    if (value > Long.MAX_VALUE || value < Long.MIN_VALUE) {
      throw new RuntimeException(
          String.format("Double value %f is out of range of long value.", value));
    }
    return Math.round(value);
  }

  public static float castDoubleToFloat(double value) {
    if (value > Float.MAX_VALUE || value < -Float.MAX_VALUE) {
      throw new RuntimeException(
          String.format("Double value %f is out of range of float value.", value));
    }
    return (float) value;
  }

  public static float castTextToFloat(String value) {
    float f = Float.parseFloat(value);
    if (f == Float.POSITIVE_INFINITY || f == Float.NEGATIVE_INFINITY) {
      throw new RuntimeException(
          String.format("Text value %s is out of range of float value.", value));
    }
    return f;
  }

  public static Double castTextToDouble(String value) {
    double d = Double.parseDouble(value);
    if (d == Double.POSITIVE_INFINITY || d == Double.NEGATIVE_INFINITY) {
      throw new RuntimeException(
          String.format("Text value %s is out of range of double value.", value));
    }
    return d;
  }

  public static boolean castTextToBoolean(String value) {
    String lowerCase = value.toLowerCase();
    if (lowerCase.equals("true")) {
      return true;
    } else if (lowerCase.equals("false")) {
      return false;
    } else {
      throw new RuntimeException(String.format("Invalid text input for boolean type: %s", value));
    }
  }
}

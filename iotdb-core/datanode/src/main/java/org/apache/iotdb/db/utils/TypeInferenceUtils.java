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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.BuiltInScalarFunctionHelper;
import org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.BuiltInScalarFunctionHelperFactory;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.enums.TSDataType;

import java.util.Collections;
import java.util.List;

public class TypeInferenceUtils {

  private static final IoTDBConfig CONF = IoTDBDescriptor.getInstance().getConfig();

  private TypeInferenceUtils() {}

  private static boolean isBlob(String s) {
    return s.length() >= 3 && s.startsWith("X'") && s.endsWith("'");
  }

  public static boolean isNumber(String s) {
    if (s == null || s.equals("NaN")) {
      return false;
    }
    try {
      Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return false;
    }
    return !s.endsWith("F") && !s.endsWith("f") && !s.endsWith("D") && !s.endsWith("d");
  }

  private static boolean isBoolean(String s) {
    return s.equalsIgnoreCase(SqlConstant.BOOLEAN_TRUE)
        || s.equalsIgnoreCase(SqlConstant.BOOLEAN_FALSE);
  }

  private static boolean isLong(String s) {
    try {
      Long.parseLong(s);
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  private static boolean isConvertFloatPrecisionLack(String s) {
    try {
      return Long.parseLong(s) > (1 << 24);
    } catch (NumberFormatException e) {
      return true;
    }
  }

  /** Get predicted DataType of the given value */
  public static TSDataType getPredictedDataType(Object value, boolean inferType) {
    if (value == null) {
      return null;
    }
    if (value instanceof Boolean) {
      return TSDataType.BOOLEAN;
    } else if (value instanceof Integer) {
      return TSDataType.INT32;
    } else if (value instanceof Long) {
      return TSDataType.INT64;
    } else if (value instanceof Float) {
      return TSDataType.FLOAT;
    } else if (value instanceof Double) {
      return TSDataType.DOUBLE;
    } else if (inferType) {
      String strValue = value.toString();
      if (isBoolean(strValue)) {
        return CONF.getBooleanStringInferType();
      } else if (isNumber(strValue)) {
        if (isLong(StringUtils.trim(strValue))) {
          return CONF.getIntegerStringInferType();
        } else {
          return CONF.getFloatingStringInferType();
        }
      } else if ("null".equals(strValue) || "NULL".equals(strValue)) {
        return null;
        // "NaN" is returned if the NaN Literal is given in Parser
      } else if ("NaN".equals(strValue)) {
        return CONF.getNanStringInferType();
      } else if (isBlob(strValue)) {
        return TSDataType.BLOB;
      } else {
        return TSDataType.TEXT;
      }
    }

    return TSDataType.TEXT;
  }

  public static TSDataType getBuiltinAggregationDataType(
      String aggregationFunctionName, TSDataType dataType) {
    if (aggregationFunctionName == null) {
      throw new IllegalArgumentException("AggregateFunction Name must not be null");
    }
    verifyIsAggregationDataTypeMatched(aggregationFunctionName, dataType);

    switch (aggregationFunctionName.toLowerCase()) {
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.COUNT:
      case SqlConstant.COUNT_TIME:
      case SqlConstant.COUNT_IF:
      case SqlConstant.TIME_DURATION:
        return TSDataType.INT64;
      case SqlConstant.MIN_VALUE:
      case SqlConstant.LAST_VALUE:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.MAX_VALUE:
      case SqlConstant.EXTREME:
      case SqlConstant.MODE:
      case SqlConstant.MAX_BY:
      case SqlConstant.MIN_BY:
        return dataType;
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.STDDEV:
      case SqlConstant.STDDEV_POP:
      case SqlConstant.STDDEV_SAMP:
      case SqlConstant.VARIANCE:
      case SqlConstant.VAR_POP:
      case SqlConstant.VAR_SAMP:
        return TSDataType.DOUBLE;
      default:
        throw new IllegalArgumentException(
            "Invalid Aggregation function: " + aggregationFunctionName);
    }
  }

  private static void verifyIsAggregationDataTypeMatched(String aggrFuncName, TSDataType dataType) {
    // input is NullOperand, needn't check
    if (dataType == null) {
      return;
    }
    switch (aggrFuncName.toLowerCase()) {
      case SqlConstant.MIN_VALUE:
      case SqlConstant.MAX_VALUE:
        if (dataType.isNumeric()
            || TSDataType.STRING.equals(dataType)
            || TSDataType.DATE.equals(dataType)
            || TSDataType.TIMESTAMP.equals(dataType)) {
          return;
        }
        throw new SemanticException(
            "Aggregate functions [MIN_VALUE, MAX_VALUE] only support data types [INT32, INT64, FLOAT, DOUBLE, STRING, DATE, TIMESTAMP]");
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.EXTREME:
      case SqlConstant.STDDEV:
      case SqlConstant.STDDEV_POP:
      case SqlConstant.STDDEV_SAMP:
      case SqlConstant.VARIANCE:
      case SqlConstant.VAR_POP:
      case SqlConstant.VAR_SAMP:
        if (dataType.isNumeric()) {
          return;
        }
        throw new SemanticException(
            "Aggregate functions [AVG, SUM, EXTREME, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]");
      case SqlConstant.COUNT:
      case SqlConstant.COUNT_TIME:
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.LAST_VALUE:
      case SqlConstant.TIME_DURATION:
      case SqlConstant.MODE:
      case SqlConstant.MAX_BY:
      case SqlConstant.MIN_BY:
        return;
      case SqlConstant.COUNT_IF:
        if (dataType != TSDataType.BOOLEAN) {
          throw new SemanticException(
              String.format(
                  "Input series of Aggregation function [%s] only supports data type [BOOLEAN]",
                  aggrFuncName));
        }
        return;
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggrFuncName);
    }
  }

  /**
   * Bind Type for non-series input Expressions of AggregationFunction and check Semantic
   *
   * <p>.e.g COUNT_IF(s1>1, keep>2, 'ignoreNull'='false'), we bind type {@link TSDataType#INT64} for
   * 'keep'
   */
  public static void bindTypeForBuiltinAggregationNonSeriesInputExpressions(
      String functionName,
      List<Expression> inputExpressions,
      List<List<Expression>> outputExpressionLists) {
    switch (functionName.toLowerCase()) {
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.EXTREME:
      case SqlConstant.MIN_VALUE:
      case SqlConstant.MAX_VALUE:
      case SqlConstant.COUNT:
      case SqlConstant.COUNT_TIME:
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.LAST_VALUE:
      case SqlConstant.TIME_DURATION:
      case SqlConstant.MODE:
      case SqlConstant.STDDEV:
      case SqlConstant.STDDEV_POP:
      case SqlConstant.STDDEV_SAMP:
      case SqlConstant.VARIANCE:
      case SqlConstant.VAR_POP:
      case SqlConstant.VAR_SAMP:
      case SqlConstant.MAX_BY:
      case SqlConstant.MIN_BY:
        return;
      case SqlConstant.COUNT_IF:
        Expression keepExpression = inputExpressions.get(1);
        if (keepExpression instanceof ConstantOperand) {
          outputExpressionLists.add(Collections.singletonList(keepExpression));
          return;
        } else if (keepExpression instanceof CompareBinaryExpression) {
          Expression leftExpression =
              ((CompareBinaryExpression) keepExpression).getLeftExpression();
          Expression rightExpression =
              ((CompareBinaryExpression) keepExpression).getRightExpression();
          if (leftExpression instanceof TimeSeriesOperand
              && leftExpression.getExpressionString().equalsIgnoreCase("keep")
              && rightExpression.isConstantOperand()) {
            outputExpressionLists.add(
                Collections.singletonList(
                    ExpressionUtils.reconstructBinaryExpression(
                        keepExpression,
                        new TimeSeriesOperand(
                            ((TimeSeriesOperand) leftExpression).getPath(), TSDataType.INT64),
                        rightExpression)));
            return;
          } else {
            throw new SemanticException(
                String.format(
                    "Please check input keep condition of Aggregation function [%s]",
                    functionName));
          }
        } else {
          throw new SemanticException(
              String.format(
                  "Keep condition of Aggregation function [%s] need to be constant or compare expression constructed by keep and a long number",
                  functionName));
        }
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + functionName);
    }
  }

  public static TSDataType getBuiltInScalarFunctionDataType(
      FunctionExpression functionExpression, TSDataType dataType) {
    String functionName = functionExpression.getFunctionName();
    if (functionName == null) {
      throw new IllegalArgumentException("ScalarFunction Name must not be null.");
    }
    BuiltInScalarFunctionHelper helper =
        BuiltInScalarFunctionHelperFactory.createHelper(functionName);
    // check input data type first if it is not a NullOperand
    if (dataType != null) {
      helper.checkBuiltInScalarFunctionInputDataType(dataType);
    }
    return helper.getBuiltInScalarFunctionReturnType(functionExpression);
  }

  public static boolean canAutoCast(TSDataType fromType, TSDataType toType) {
    if (fromType.equals(toType)) {
      return true;
    }

    switch (fromType) {
      case INT32:
        switch (toType) {
          case INT64:
          case FLOAT:
          case DOUBLE:
            return true;
          default:
            return false;
        }
      case INT64:
      case FLOAT:
        return toType.equals(TSDataType.DOUBLE);
      case DOUBLE:
      case BOOLEAN:
      case TEXT:
      case DATE:
      case TIMESTAMP:
      case BLOB:
      case STRING:
        return false;
      default:
        throw new IllegalArgumentException("Unknown data type: " + fromType);
    }
  }
}

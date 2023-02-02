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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.lang3.StringUtils;

public class TypeInferenceUtils {

  private static final TSDataType booleanStringInferType =
      IoTDBDescriptor.getInstance().getConfig().getBooleanStringInferType();

  private static final TSDataType integerStringInferType =
      IoTDBDescriptor.getInstance().getConfig().getIntegerStringInferType();

  private static final TSDataType longStringInferType =
      IoTDBDescriptor.getInstance().getConfig().getLongStringInferType();

  private static final TSDataType floatingStringInferType =
      IoTDBDescriptor.getInstance().getConfig().getFloatingStringInferType();

  private static final TSDataType nanStringInferType =
      IoTDBDescriptor.getInstance().getConfig().getNanStringInferType();

  private TypeInferenceUtils() {}

  static boolean isNumber(String s) {
    if (s == null || s.equals("NaN")) {
      return false;
    }
    try {
      Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  private static boolean isBoolean(String s) {
    return s.equalsIgnoreCase(SqlConstant.BOOLEAN_TRUE)
        || s.equalsIgnoreCase(SqlConstant.BOOLEAN_FALSE);
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

    if (inferType) {
      String strValue = value.toString();
      if (isBoolean(strValue)) {
        return booleanStringInferType;
      } else if (isNumber(strValue)) {
        if (!strValue.contains(TsFileConstant.PATH_SEPARATOR)) {
          if (isConvertFloatPrecisionLack(StringUtils.trim(strValue))) {
            return longStringInferType;
          }
          return integerStringInferType;
        } else {
          return floatingStringInferType;
        }
      } else if ("null".equals(strValue) || "NULL".equals(strValue)) {
        return null;
        // "NaN" is returned if the NaN Literal is given in Parser
      } else if ("NaN".equals(strValue)) {
        return nanStringInferType;
      } else {
        return TSDataType.TEXT;
      }
    } else if (value instanceof Boolean) {
      return TSDataType.BOOLEAN;
    } else if (value instanceof Integer) {
      return TSDataType.INT32;
    } else if (value instanceof Long) {
      return TSDataType.INT64;
    } else if (value instanceof Float) {
      return TSDataType.FLOAT;
    } else if (value instanceof Double) {
      return TSDataType.DOUBLE;
    }

    return TSDataType.TEXT;
  }

  public static TSDataType getAggrDataType(String aggrFuncName, TSDataType dataType) {
    if (aggrFuncName == null) {
      throw new IllegalArgumentException("AggregateFunction Name must not be null");
    }
    if (!verifyIsAggregationDataTypeMatched(aggrFuncName, dataType)) {
      throw new SemanticException(
          "Aggregate functions [AVG, SUM, EXTREME, MIN_VALUE, MAX_VALUE] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]");
    }

    switch (aggrFuncName.toLowerCase()) {
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.COUNT:
        return TSDataType.INT64;
      case SqlConstant.MIN_VALUE:
      case SqlConstant.LAST_VALUE:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.MAX_VALUE:
      case SqlConstant.EXTREME:
        return dataType;
      case SqlConstant.AVG:
      case SqlConstant.SUM:
        return TSDataType.DOUBLE;
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggrFuncName);
    }
  }

  private static boolean verifyIsAggregationDataTypeMatched(
      String aggrFuncName, TSDataType dataType) {
    // input is NullOperand, needn't check
    if (dataType == null) {
      return true;
    }
    switch (aggrFuncName.toLowerCase()) {
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.EXTREME:
      case SqlConstant.MIN_VALUE:
      case SqlConstant.MAX_VALUE:
        return dataType.isNumeric();
      case SqlConstant.COUNT:
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.LAST_VALUE:
        return true;
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggrFuncName);
    }
  }

  public static TSDataType getScalarFunctionDataType(String funcName, TSDataType dataType) {
    if (funcName == null) {
      throw new IllegalArgumentException("ScalarFunction Name must not be null");
    }
    verifyIsScalarFunctionDataTypeMatched(funcName, dataType);

    switch (funcName.toLowerCase()) {
      case SqlConstant.DIFF:
        return TSDataType.DOUBLE;
      default:
        throw new IllegalArgumentException("Invalid Scalar function: " + funcName);
    }
  }

  private static void verifyIsScalarFunctionDataTypeMatched(String funcName, TSDataType dataType) {
    // input is NullOperand, needn't check
    if (dataType == null) {
      return;
    }
    switch (funcName.toLowerCase()) {
      case SqlConstant.DIFF:
        if (dataType.isNumeric()) {
          return;
        }
        throw new SemanticException(
            String.format(
                "Scalar function [%s] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]",
                funcName));
      default:
        throw new IllegalArgumentException("Invalid Scalar function: " + funcName);
    }
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
        return false;
      default:
        throw new IllegalArgumentException("Unknown data type: " + fromType);
    }
  }
}

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
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class TypeInferenceUtils {

  private static TSDataType booleanStringInferType =
      IoTDBDescriptor.getInstance().getConfig().getBooleanStringInferType();

  private static TSDataType integerStringInferType =
      IoTDBDescriptor.getInstance().getConfig().getIntegerStringInferType();

  private static TSDataType longStringInferType =
      IoTDBDescriptor.getInstance().getConfig().getLongStringInferType();

  private static TSDataType floatingStringInferType =
      IoTDBDescriptor.getInstance().getConfig().getFloatingStringInferType();

  private static TSDataType nanStringInferType =
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
    return s.equalsIgnoreCase(SQLConstant.BOOLEAN_TRUE)
        || s.equalsIgnoreCase(SQLConstant.BOOLEAN_FALSE);
  }

  private static boolean isConvertFloatPrecisionLack(String s) {
    return Long.parseLong(s) > (2 << 24);
  }

  /** Get predicted DataType of the given value */
  public static TSDataType getPredictedDataType(Object value, boolean inferType) {

    if (inferType) {
      String strValue = value.toString();
      if (isBoolean(strValue)) {
        return booleanStringInferType;
      } else if (isNumber(strValue)) {
        if (!strValue.contains(TsFileConstant.PATH_SEPARATOR)) {
          if (isConvertFloatPrecisionLack(strValue)) {
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

    switch (aggrFuncName.toLowerCase()) {
      case SQLConstant.MIN_TIME:
      case SQLConstant.MAX_TIME:
      case SQLConstant.COUNT:
        return TSDataType.INT64;
      case SQLConstant.MIN_VALUE:
      case SQLConstant.LAST_VALUE:
      case SQLConstant.FIRST_VALUE:
      case SQLConstant.MAX_VALUE:
        return dataType;
      case SQLConstant.AVG:
      case SQLConstant.SUM:
        return TSDataType.DOUBLE;
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggrFuncName);
    }
  }
}

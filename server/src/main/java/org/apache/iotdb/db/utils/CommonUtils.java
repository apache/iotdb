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

import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

@SuppressWarnings("java:S106") // for console outputs
public class CommonUtils {

  private static final String ERROR_MSG = "data type is not consistent, input %s, registered %s";

  private CommonUtils() {}

  public static Object parseValue(TSDataType dataType, String value) throws QueryProcessException {
    try {
      if ("null".equals(value) || "NULL".equals(value)) {
        return null;
      }
      switch (dataType) {
        case BOOLEAN:
          return parseBoolean(value);
        case INT32:
          return Integer.parseInt(StringUtils.trim(value));
        case INT64:
          return Long.parseLong(StringUtils.trim(value));
        case FLOAT:
          float f = Float.parseFloat(value);
          if (Float.isInfinite(f)) {
            throw new QueryProcessException("The input float value is Infinity");
          }
          return f;
        case DOUBLE:
          double d = Double.parseDouble(value);
          if (Double.isInfinite(d)) {
            throw new QueryProcessException("The input double value is Infinity");
          }
          return d;
        case TEXT:
          return parseBinary(value);
        default:
          throw new QueryProcessException("Unsupported data type:" + dataType);
      }
    } catch (NumberFormatException e) {
      throw new QueryProcessException(String.format(ERROR_MSG, value, dataType));
    }
  }

  private static Binary parseBinary(String value) {
    if ((value.startsWith(SqlConstant.QUOTE) && value.endsWith(SqlConstant.QUOTE))
        || (value.startsWith(SqlConstant.DQUOTE) && value.endsWith(SqlConstant.DQUOTE))) {
      if (value.length() == 1) {
        return new Binary(value);
      } else {
        return new Binary(value.substring(1, value.length() - 1));
      }
    }

    return new Binary(value);
  }

  public static boolean checkCanCastType(TSDataType src, TSDataType dest) {
    switch (src) {
      case INT32:
        return dest == TSDataType.INT64 || dest == TSDataType.FLOAT || dest == TSDataType.DOUBLE;
      case INT64:
      case FLOAT:
        return dest == TSDataType.DOUBLE;
      default:
        return false;
    }
  }

  public static Object castValue(TSDataType srcDataType, TSDataType destDataType, Object value) {
    switch (srcDataType) {
      case INT32:
        if (destDataType == TSDataType.INT64) {
          value = (long) ((int) value);
        } else if (destDataType == TSDataType.FLOAT) {
          value = (float) ((int) value);
        } else if (destDataType == TSDataType.DOUBLE) {
          value = (double) ((int) value);
        }
        return value;
      case INT64:
        if (destDataType == TSDataType.DOUBLE) {
          value = (double) ((long) value);
        }
        return value;
      case FLOAT:
        if (destDataType == TSDataType.DOUBLE) {
          value = (double) ((float) value);
        }
        return value;
      default:
        return value;
    }
  }

  public static Object castArray(TSDataType srcDataType, TSDataType destDataType, Object value) {
    switch (srcDataType) {
      case INT32:
        if (destDataType == TSDataType.INT64) {
          value = Arrays.stream((int[]) value).mapToLong(Long::valueOf).toArray();
        } else if (destDataType == TSDataType.FLOAT) {
          int[] tmp = (int[]) value;
          float[] result = new float[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = (float) tmp[i];
          }
          value = result;
        } else if (destDataType == TSDataType.DOUBLE) {
          value = Arrays.stream((int[]) value).mapToDouble(Double::valueOf).toArray();
        }
        return value;
      case INT64:
        if (destDataType == TSDataType.DOUBLE) {
          value = Arrays.stream((long[]) value).mapToDouble(Double::valueOf).toArray();
        }
        return value;
      case FLOAT:
        if (destDataType == TSDataType.DOUBLE) {
          float[] tmp = (float[]) value;
          double[] result = new double[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = tmp[i];
          }
          value = result;
        }
        return value;
      default:
        return value;
    }
  }

  private static boolean parseBoolean(String value) throws QueryProcessException {
    value = value.toLowerCase();
    if (SqlConstant.BOOLEAN_FALSE_NUM.equals(value) || SqlConstant.BOOLEAN_FALSE.equals(value)) {
      return false;
    }
    if (SqlConstant.BOOLEAN_TRUE_NUM.equals(value) || SqlConstant.BOOLEAN_TRUE.equals(value)) {
      return true;
    }
    throw new QueryProcessException("The BOOLEAN should be true/TRUE, false/FALSE or 0/1");
  }
}

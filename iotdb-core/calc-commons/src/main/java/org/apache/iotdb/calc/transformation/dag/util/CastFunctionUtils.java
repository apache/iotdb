/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.calc.transformation.dag.util;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;

import javax.annotation.Nonnull;

import java.time.ZoneId;
import java.time.format.DateTimeParseException;

public class CastFunctionUtils {
  public static final String ERROR_MSG = "Unsupported target dataType: %s";

  public static int castLongToInt(long value) {
    if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
      throw new SemanticException(
          String.format("long value %d is out of range of integer value.", value));
    }
    return (int) value;
  }

  public static int castFloatToInt(float value) {
    if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
      throw new SemanticException(
          String.format("Float value %f is out of range of integer value.", value));
    }
    return Math.round(value);
  }

  public static long castFloatToLong(float value) {
    if (value > Long.MAX_VALUE || value < Long.MIN_VALUE) {
      throw new SemanticException(
          String.format("Float value %f is out of range of long value.", value));
    }
    return Math.round((double) value);
  }

  public static int castDoubleToInt(double value) {
    if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
      throw new SemanticException(
          String.format("Double value %f is out of range of integer value.", value));
    }
    return Math.round((float) value);
  }

  public static long castDoubleToLong(double value) {
    if (value > Long.MAX_VALUE || value < Long.MIN_VALUE) {
      throw new SemanticException(
          String.format("Double value %f is out of range of long value.", value));
    }
    return Math.round(value);
  }

  public static float castDoubleToFloat(double value) {
    if (value > Float.MAX_VALUE || value < -Float.MAX_VALUE) {
      throw new SemanticException(
          String.format("Double value %f is out of range of float value.", value));
    }
    return (float) value;
  }

  public static float castTextToFloat(String value) {
    float f = Float.parseFloat(value);
    if (f == Float.POSITIVE_INFINITY || f == Float.NEGATIVE_INFINITY) {
      throw new SemanticException(
          String.format("Text value %s is out of range of float value.", value));
    }
    return f;
  }

  public static double castTextToDouble(String value) {
    double d = Double.parseDouble(value);
    if (d == Double.POSITIVE_INFINITY || d == Double.NEGATIVE_INFINITY) {
      throw new SemanticException(
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
      throw new SemanticException(String.format("Invalid text input for boolean type: %s", value));
    }
  }

  // used by IrExpressionInterpreter to do constant folding
  public static Object cast(
      @Nonnull Object value, Type sourceType, Type targetType, SessionInfo session) {
    switch (sourceType.getTypeEnum()) {
      case INT32:
        int intV = value instanceof Integer ? (int) value : ((Long) value).intValue();
        return castInt(intV, targetType);
      case DATE:
        int dateV = value instanceof Integer ? (int) value : ((Long) value).intValue();
        return castDate(dateV, targetType, session.getZoneId());
      case INT64:
        long longV = (Long) value;
        return castLong(longV, targetType);
      case TIMESTAMP:
        long timestampV = (Long) value;
        return castTimestamp(timestampV, targetType, session.getZoneId());
      case FLOAT:
        float floatV = value instanceof Float ? (float) value : ((Double) value).floatValue();
        return castFloat(floatV, targetType);
      case DOUBLE:
        double doubleV = (Double) value;
        return castDouble(doubleV, targetType);
      case BOOLEAN:
        boolean boolV = (Boolean) value;
        return castBool(boolV, targetType);
      case TEXT:
      case STRING:
      case BLOB:
        Binary binaryV = (Binary) value;
        return castBinary(binaryV, targetType, session.getZoneId());
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported source dataType: %s", sourceType.getTypeEnum()));
    }
  }

  private static Object castInt(int value, Type targetType) {
    switch (targetType.getTypeEnum()) {
      case INT32:
      case DATE:
        return value;
      case INT64:
      case TIMESTAMP:
        return (long) value;
      case FLOAT:
        return (float) value;
      case DOUBLE:
        return (double) value;
      case BOOLEAN:
        return value != 0;
      case TEXT:
      case STRING:
        return BytesUtils.valueOf(String.valueOf(value));
      case BLOB:
        return new Binary(BytesUtils.intToBytes(value));
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, targetType.getTypeEnum()));
    }
  }

  private static Object castDate(int value, Type targetType, ZoneId zoneId) {
    switch (targetType.getTypeEnum()) {
      case INT32:
      case DATE:
        return value;
      case INT64:
        return (long) value;
      case TIMESTAMP:
        return DateTimeUtils.correctPrecision(DateUtils.parseIntToTimestamp(value, zoneId));
      case FLOAT:
        return (float) value;
      case DOUBLE:
        return (double) value;
      case BOOLEAN:
        return value != 0;
      case TEXT:
      case STRING:
        return BytesUtils.valueOf(DateUtils.formatDate(value));
      case BLOB:
        return new Binary(BytesUtils.intToBytes(value));
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, targetType.getTypeEnum()));
    }
  }

  private static Object castLong(long value, Type targetType) {
    switch (targetType.getTypeEnum()) {
      case INT32:
      case DATE:
        return castLongToInt(value);
      case INT64:
      case TIMESTAMP:
        return value;
      case FLOAT:
        return (float) value;
      case DOUBLE:
        return (double) value;
      case BOOLEAN:
        return value != 0L;
      case TEXT:
      case STRING:
        return BytesUtils.valueOf(String.valueOf(value));
      case BLOB:
        return new Binary(BytesUtils.longToBytes(value));
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, targetType.getTypeEnum()));
    }
  }

  private static Object castTimestamp(long value, Type targetType, ZoneId zoneId) {
    switch (targetType.getTypeEnum()) {
      case INT32:
        return castLongToInt(value);
      case DATE:
        return DateUtils.parseDateExpressionToInt(DateTimeUtils.convertToLocalDate(value, zoneId));
      case INT64:
      case TIMESTAMP:
        return value;
      case FLOAT:
        return (float) value;
      case DOUBLE:
        return (double) value;
      case BOOLEAN:
        return value != 0L;
      case TEXT:
      case STRING:
        return BytesUtils.valueOf(DateTimeUtils.convertLongToDate(value, zoneId));
      case BLOB:
        return new Binary(BytesUtils.longToBytes(value));
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, targetType.getTypeEnum()));
    }
  }

  private static Object castFloat(float value, Type targetType) {
    switch (targetType.getTypeEnum()) {
      case INT32:
      case DATE:
        return castFloatToInt(value);
      case INT64:
      case TIMESTAMP:
        return castFloatToLong(value);
      case FLOAT:
        return value;
      case DOUBLE:
        return (double) value;
      case BOOLEAN:
        return value != 0.0f;
      case TEXT:
      case STRING:
        return BytesUtils.valueOf(String.valueOf(value));
      case BLOB:
        return new Binary(BytesUtils.floatToBytes(value));
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, targetType.getTypeEnum()));
    }
  }

  private static Object castDouble(double value, Type targetType) {
    switch (targetType.getTypeEnum()) {
      case INT32:
      case DATE:
        return castDoubleToInt(value);
      case INT64:
      case TIMESTAMP:
        return castDoubleToLong(value);
      case FLOAT:
        return castDoubleToFloat(value);
      case DOUBLE:
        return value;
      case BOOLEAN:
        return value != 0.0d;
      case TEXT:
      case STRING:
        return BytesUtils.valueOf(String.valueOf(value));
      case BLOB:
        return new Binary(BytesUtils.doubleToBytes(value));
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, targetType.getTypeEnum()));
    }
  }

  private static Object castBool(boolean value, Type targetType) {
    switch (targetType.getTypeEnum()) {
      case INT32:
      case DATE:
        return value ? 1 : 0;
      case INT64:
      case TIMESTAMP:
        return value ? 1L : 0L;
      case FLOAT:
        return value ? 1.0f : 0.0f;
      case DOUBLE:
        return value ? 1.0d : 0.0d;
      case BOOLEAN:
        return value;
      case TEXT:
      case STRING:
        return BytesUtils.valueOf(String.valueOf(value));
      case BLOB:
        return new Binary(BytesUtils.boolToBytes(value));
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, targetType.getTypeEnum()));
    }
  }

  private static Object castBinary(Binary value, Type targetType, ZoneId zoneId) {
    String stringValue = value.getStringValue(TSFileConfig.STRING_CHARSET);
    try {
      switch (targetType.getTypeEnum()) {
        case INT32:
          return Integer.parseInt(stringValue);
        case DATE:
          return DateUtils.parseDateExpressionToInt(stringValue);
        case INT64:
          return Long.parseLong(stringValue);
        case TIMESTAMP:
          return DateTimeUtils.convertDatetimeStrToLong(stringValue, zoneId);
        case FLOAT:
          return castTextToFloat(stringValue);
        case DOUBLE:
          return castTextToDouble(stringValue);
        case BOOLEAN:
          return castTextToBoolean(stringValue);
        case TEXT:
        case STRING:
        case BLOB:
          return value;
        default:
          throw new UnsupportedOperationException(
              String.format(ERROR_MSG, targetType.getTypeEnum()));
      }
    } catch (DateTimeParseException | NumberFormatException e) {
      throw new SemanticException(
          String.format("Cannot cast %s to %s type", stringValue, targetType.getDisplayName()));
    }
  }
}

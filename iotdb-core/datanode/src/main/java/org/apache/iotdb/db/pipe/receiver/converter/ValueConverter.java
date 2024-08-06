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

package org.apache.iotdb.db.pipe.receiver.converter;

import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;

import java.time.ZoneId;

public class ValueConverter {

  ////////////// BOOLEAN //////////////

  private static final Binary BINARY_TRUE = parseString(Boolean.TRUE.toString());
  private static final Binary BINARY_FALSE = parseString(Boolean.FALSE.toString());

  public static int convertBooleanToInt32(final boolean value) {
    return value ? 1 : 0;
  }

  public static long convertBooleanToInt64(final boolean value) {
    return value ? 1L : 0L;
  }

  public static float convertBooleanToFloat(final boolean value) {
    return value ? 1.0f : 0.0f;
  }

  public static double convertBooleanToDouble(final boolean value) {
    return value ? 1.0 : 0.0;
  }

  public static Binary convertBooleanToText(final boolean value) {
    return value ? BINARY_TRUE : BINARY_FALSE;
  }

  public static long convertBooleanToTimestamp(final boolean value) {
    return value ? 1L : 0L;
  }

  public static int convertBooleanToDate(final boolean value) {
    return value ? 1 : 0;
  }

  public static Binary convertBooleanToBlob(final boolean value) {
    return value ? BINARY_TRUE : BINARY_FALSE;
  }

  public static Binary convertBooleanToString(final boolean value) {
    return value ? BINARY_TRUE : BINARY_FALSE;
  }

  ///////////// INT32 //////////////

  public static boolean convertInt32ToBoolean(final int value) {
    return value != 0;
  }

  public static long convertInt32ToInt64(final int value) {
    return value;
  }

  public static float convertInt32ToFloat(final int value) {
    return value;
  }

  public static double convertInt32ToDouble(final int value) {
    return value;
  }

  public static Binary convertInt32ToText(final int value) {
    return parseText(Integer.toString(value));
  }

  public static long convertInt32ToTimestamp(final int value) {
    return value;
  }

  public static int convertInt32ToDate(final int value) {
    return value;
  }

  public static Binary convertInt32ToBlob(final int value) {
    return parseBlob(Integer.toString(value));
  }

  public static Binary convertInt32ToString(final int value) {
    return parseString(Integer.toString(value));
  }

  ///////////// INT64 //////////////

  public static boolean convertInt64ToBoolean(final long value) {
    return value != 0;
  }

  public static int convertInt64ToInt32(final long value) {
    return (int) value;
  }

  public static float convertInt64ToFloat(final long value) {
    return value;
  }

  public static double convertInt64ToDouble(final long value) {
    return value;
  }

  public static Binary convertInt64ToText(final long value) {
    return parseText(Long.toString(value));
  }

  public static long convertInt64ToTimestamp(final long value) {
    return value;
  }

  public static int convertInt64ToDate(final long value) {
    return (int) value;
  }

  public static Binary convertInt64ToBlob(final long value) {
    return parseBlob(Long.toString(value));
  }

  public static Binary convertInt64ToString(final long value) {
    return parseString(Long.toString(value));
  }

  ///////////// FLOAT //////////////

  public static boolean convertFloatToBoolean(final float value) {
    return value != 0;
  }

  public static int convertFloatToInt32(final float value) {
    return (int) value;
  }

  public static long convertFloatToInt64(final float value) {
    return (long) value;
  }

  public static double convertFloatToDouble(final float value) {
    return value;
  }

  public static Binary convertFloatToText(final float value) {
    return parseText(Float.toString(value));
  }

  public static long convertFloatToTimestamp(final float value) {
    return (long) value;
  }

  public static int convertFloatToDate(final float value) {
    return (int) value;
  }

  public static Binary convertFloatToBlob(final float value) {
    return parseBlob(Float.toString(value));
  }

  public static Binary convertFloatToString(final float value) {
    return parseString(Float.toString(value));
  }

  ///////////// DOUBLE //////////////

  public static boolean convertDoubleToBoolean(final double value) {
    return value != 0;
  }

  public static int convertDoubleToInt32(final double value) {
    return (int) value;
  }

  public static long convertDoubleToInt64(final double value) {
    return (long) value;
  }

  public static float convertDoubleToFloat(final double value) {
    return (float) value;
  }

  public static Binary convertDoubleToText(final double value) {
    return parseText(Double.toString(value));
  }

  public static long convertDoubleToTimestamp(final double value) {
    return (long) value;
  }

  public static int convertDoubleToDate(final double value) {
    return (int) value;
  }

  public static Binary convertDoubleToBlob(final double value) {
    return parseBlob(Double.toString(value));
  }

  public static Binary convertDoubleToString(final double value) {
    return parseString(Double.toString(value));
  }

  ///////////// TEXT //////////////

  public static boolean convertTextToBoolean(final Binary value) {
    return Boolean.parseBoolean(value.toString());
  }

  public static int convertTextToInt32(final Binary value) {
    return parseInteger(value.toString());
  }

  public static long convertTextToInt64(final Binary value) {
    return parseLong(value.toString());
  }

  public static float convertTextToFloat(final Binary value) {
    return parseFloat(value.toString());
  }

  public static double convertTextToDouble(final Binary value) {
    return parseDouble(value.toString());
  }

  public static long convertTextToTimestamp(final Binary value) {
    return parseTimestamp(value.toString());
  }

  public static int convertTextToDate(final Binary value) {
    return parseDate(value.toString());
  }

  public static Binary convertTextToBlob(final Binary value) {
    return parseBlob(value.toString());
  }

  public static Binary convertTextToString(final Binary value) {
    return value;
  }

  ///////////// TIMESTAMP //////////////

  public static boolean convertTimestampToBoolean(final long value) {
    return value != 0;
  }

  public static int convertTimestampToInt32(final long value) {
    return (int) value;
  }

  public static long convertTimestampToInt64(final long value) {
    return value;
  }

  public static float convertTimestampToFloat(final long value) {
    return value;
  }

  public static double convertTimestampToDouble(final long value) {
    return value;
  }

  public static Binary convertTimestampToText(final long value) {
    return parseText(Long.toString(value));
  }

  public static int convertTimestampToDate(final long value) {
    return (int) value;
  }

  public static Binary convertTimestampToBlob(final long value) {
    return parseBlob(Long.toString(value));
  }

  public static Binary convertTimestampToString(final long value) {
    return parseString(Long.toString(value));
  }

  ///////////// DATE //////////////

  public static boolean convertDateToBoolean(final int value) {
    return value != 0;
  }

  public static int convertDateToInt32(final int value) {
    return value;
  }

  public static long convertDateToInt64(final int value) {
    return value;
  }

  public static float convertDateToFloat(final int value) {
    return value;
  }

  public static double convertDateToDouble(final int value) {
    return value;
  }

  public static Binary convertDateToText(final int value) {
    return parseText(Integer.toString(value));
  }

  public static long convertDateToTimestamp(final int value) {
    return value;
  }

  public static Binary convertDateToBlob(final int value) {
    return parseBlob(Integer.toString(value));
  }

  public static Binary convertDateToString(final int value) {
    return parseString(Integer.toString(value));
  }

  ///////////// BLOB //////////////

  public static boolean convertBlobToBoolean(final Binary value) {
    return Boolean.parseBoolean(value.toString());
  }

  public static int convertBlobToInt32(final Binary value) {
    return parseInteger(value.toString());
  }

  public static long convertBlobToInt64(final Binary value) {
    return parseLong(value.toString());
  }

  public static float convertBlobToFloat(final Binary value) {
    return parseFloat(value.toString());
  }

  public static double convertBlobToDouble(final Binary value) {
    return parseDouble(value.toString());
  }

  public static long convertBlobToTimestamp(final Binary value) {
    return parseTimestamp(value.toString());
  }

  public static int convertBlobToDate(final Binary value) {
    return parseDate(value.toString());
  }

  public static Binary convertBlobToString(final Binary value) {
    return value;
  }

  public static Binary convertBlobToText(final Binary value) {
    return value;
  }

  ///////////// STRING //////////////

  public static boolean convertStringToBoolean(final Binary value) {
    return Boolean.parseBoolean(value.toString());
  }

  public static int convertStringToInt32(final Binary value) {
    return parseInteger(value.toString());
  }

  public static long convertStringToInt64(final Binary value) {
    return parseLong(value.toString());
  }

  public static float convertStringToFloat(final Binary value) {
    return parseFloat(value.toString());
  }

  public static double convertStringToDouble(final Binary value) {
    return parseDouble(value.toString());
  }

  public static long convertStringToTimestamp(final Binary value) {
    return parseTimestamp(value.toString());
  }

  public static int convertStringToDate(final Binary value) {
    return parseDate(value.toString());
  }

  public static Binary convertStringToBlob(final Binary value) {
    return parseBlob(value.toString());
  }

  public static Binary convertStringToText(final Binary value) {
    return value;
  }

  ///////////// UTILS //////////////

  private static Binary parseBlob(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  private static int parseInteger(final String value) {
    try {
      return Integer.parseInt(value);
    } catch (Exception e) {
      return 0;
    }
  }

  private static long parseLong(final String value) {
    try {
      return Long.parseLong(value);
    } catch (Exception e) {
      return 0L;
    }
  }

  private static float parseFloat(final String value) {
    try {
      return Float.parseFloat(value);
    } catch (Exception e) {
      return 0.0f;
    }
  }

  private static double parseDouble(final String value) {
    try {
      return Double.parseDouble(value);
    } catch (Exception e) {
      return 0.0d;
    }
  }

  private static long parseTimestamp(final String value) {
    if (value == null || value.isEmpty()) {
      return 0L;
    }
    try {
      return TypeInferenceUtils.isNumber(value)
          ? Long.parseLong(value)
          : DateTimeUtils.parseDateTimeExpressionToLong(
              StringUtils.trim(value), ZoneId.systemDefault());
    } catch (final Exception e) {
      return 0L;
    }
  }

  private static int parseDate(final String value) {
    if (value == null || value.isEmpty()) {
      return 0;
    }
    try {
      return TypeInferenceUtils.isNumber(value)
          ? Integer.parseInt(value)
          : DateTimeUtils.parseDateExpressionToInt(StringUtils.trim(value));
    } catch (final Exception e) {
      return 0;
    }
  }

  private static Binary parseString(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  private static Binary parseText(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  private ValueConverter() {
    // forbidden to construct
  }
}

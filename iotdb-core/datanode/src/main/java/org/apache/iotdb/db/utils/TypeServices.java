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

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.function.Function;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

public class TypeServices {

  public static final int DEFAULT_DATE =
      DateUtils.parseDateExpressionToInt(LocalDate.of(1970, 1, 1));

  public static final TypeService<Function<String, Object>>
      VALUE_PARSER_NO_EXCEPTION_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> Boolean::parseBoolean;
            case INT32 -> TypeServices::parseInteger;
            case INT64 -> TypeServices::parseLong;
            case FLOAT -> TypeServices::parseFloat;
            case DOUBLE -> TypeServices::parseDouble;
            case TEXT -> TypeServices::parseText;
            case TIMESTAMP -> TypeServices::parseTimestamp;
            case DATE -> TypeServices::parseDate;
            case BLOB -> TypeServices::parseBlob;
            case STRING -> TypeServices::parseString;
            default -> throw new UnSupportedDataTypeException(
                CalcMessages.UNKNOWN_DATATYPE + type);
          };

  static {
    VALUE_PARSER_NO_EXCEPTION_SERVICE.check();
  }

  public static int parseInteger(final String value) {
    try {
      return Integer.parseInt(value);
    } catch (final Exception e) {
      return 0;
    }
  }

  public static long parseLong(final String value) {
    try {
      return Long.parseLong(value);
    } catch (final Exception e) {
      return 0L;
    }
  }

  public static float parseFloat(final String value) {
    try {
      return Float.parseFloat(value);
    } catch (final Exception e) {
      return 0.0f;
    }
  }

  public static double parseDouble(final String value) {
    try {
      return Double.parseDouble(value);
    } catch (final Exception e) {
      return 0.0d;
    }
  }

  public static Binary parseBlob(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  public static Binary parseString(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  public static Binary parseText(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  public static long parseTimestamp(final String value) {
    if (value == null || value.isEmpty()) {
      return 0L;
    }
    try {
      return TypeInferenceUtils.isNumber(value)
          ? Long.parseLong(value)
          : DataNodeDateTimeUtils.parseDateTimeExpressionToLong(
              StringUtils.trim(value), ZoneOffset.UTC);
    } catch (final Exception e) {
      return 0L;
    }
  }

  public static int parseDate(final String value) {
    if (value == null) {
      return DEFAULT_DATE;
    }
    final String trimmedValue = StringUtils.trim(value);
    if (trimmedValue.isEmpty()) {
      return DEFAULT_DATE;
    }
    if (TypeInferenceUtils.isNumber(trimmedValue)) {
      try {
        int date = Integer.parseInt(trimmedValue);
        DateUtils.parseIntToLocalDate(date);
        return date;
      } catch (final Exception e) {
        return DEFAULT_DATE;
      }
    }
    try {
      return DateTimeUtils.parseDateExpressionToInt(trimmedValue);
    } catch (final Exception e) {
      return parseDateTimeToDate(trimmedValue);
    }
  }

  public static int parseDateTimeToDate(final String value) {
    try {
      return DateUtils.parseDateExpressionToInt(
          Instant.ofEpochMilli(
                  DateTimeUtils.convertDatetimeStrToLong(value, ZoneOffset.UTC, 0, "ms"))
              .atZone(ZoneOffset.UTC)
              .toLocalDate());
    } catch (final Exception e) {
      return DEFAULT_DATE;
    }
  }
}

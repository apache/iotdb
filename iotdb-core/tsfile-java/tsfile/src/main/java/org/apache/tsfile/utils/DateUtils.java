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

package org.apache.tsfile.utils;

import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateUtils {
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  public static String formatDate(int date) {
    return date / 10000
        + "-"
        + String.format("%02d", (date / 100) % 100)
        + "-"
        + String.format("%02d", date % 100);
  }

  public static Integer parseDateExpressionToInt(String dateExpression) {
    if (dateExpression == null || dateExpression.isEmpty()) {
      throw new DateTimeParseException("Date expression is null or empty.", "", 0);
    }
    LocalDate date;
    try {
      date = LocalDate.parse(dateExpression, DATE_FORMATTER);
    } catch (DateTimeParseException e) {
      throw new DateTimeParseException(
          "Invalid date format. Please use YYYY-MM-DD format.", dateExpression, 0);
    }
    if (date.getYear() < 1000 || date.getYear() > 9999) {
      throw new DateTimeParseException("Year must be between 1000 and 9999.", dateExpression, 0);
    }
    return date.getYear() * 10000 + date.getMonthValue() * 100 + date.getDayOfMonth();
  }

  public static Integer parseDateExpressionToInt(LocalDate localDate) {
    if (localDate == null) {
      throw new DateTimeParseException("Date expression is null or empty.", "", 0);
    }
    if (localDate.getYear() < 1000 || localDate.getYear() > 9999) {
      throw new DateTimeParseException(
          "Year must be between 1000 and 9999.", localDate.format(DATE_FORMATTER), 0);
    }
    return localDate.getYear() * 10000
        + localDate.getMonthValue() * 100
        + localDate.getDayOfMonth();
  }

  // Do not use this method to get a timestamp, use parseIntToTimestamp instead.
  public static Date parseIntToDate(int date) {
    return new Date(date / 10000 - 1900, (date / 100) % 100 - 1, date % 100);
  }

  public static LocalDate parseIntToLocalDate(int date) {
    try {
      return LocalDate.of(date / 10000, (date / 100) % 100, date % 100);
    } catch (Exception e) {
      throw new DateTimeParseException("Invalid date format.", "", 0);
    }
  }

  public static long parseIntToTimestamp(int date, ZoneId zoneId) {
    return ZonedDateTime.of(date / 10000, (date / 100) % 100, date % 100, 0, 0, 0, 0, zoneId)
        .toInstant()
        .toEpochMilli();
  }
}

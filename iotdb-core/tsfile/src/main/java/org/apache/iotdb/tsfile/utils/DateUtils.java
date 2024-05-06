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

package org.apache.iotdb.tsfile.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateUtils {
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  public static String formatDate(int date) {
    String dateStr = String.valueOf(date);
    String year = dateStr.substring(0, 4);
    String month = dateStr.substring(4, 6);
    String day = dateStr.substring(6, 8);
    return year + "-" + month + "-" + day;
  }

  public static Integer parseDateExpressionToInt(String dateExpression) {
    try {
      LocalDate date = LocalDate.parse(dateExpression, DATE_FORMATTER);
      return date.getYear() * 10000 + date.getMonthValue() * 100 + date.getDayOfMonth();
    } catch (DateTimeParseException e) {
      throw new DateTimeParseException(
          "Invalid date format. Please use YYYY-MM-DD format.", dateExpression, 0);
    }
  }
}

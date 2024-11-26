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

import org.junit.Test;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DateUtilsTest {

  @Test
  public void testFormatDate() {
    int date = 20230514;
    String formattedDate = DateUtils.formatDate(date);
    assertEquals("2023-05-14", formattedDate);
  }

  @Test
  public void testParseDateExpressionToInt_ValidDate() {
    String dateExpression = "2023-05-14";
    int dateInt = DateUtils.parseDateExpressionToInt(dateExpression);
    assertEquals(20230514, dateInt);
  }

  @Test
  public void testParseDateExpressionToInt_InvalidDate() {
    String dateExpression = "2023-14-05";
    assertThrows(
        DateTimeParseException.class,
        () -> {
          DateUtils.parseDateExpressionToInt(dateExpression);
        });
  }

  @Test
  public void testParseDateExpressionToInt_NullOrEmpty() {
    assertThrows(
        DateTimeParseException.class,
        () -> {
          DateUtils.parseDateExpressionToInt((String) null);
        });
    assertThrows(
        DateTimeParseException.class,
        () -> {
          DateUtils.parseDateExpressionToInt("");
        });
  }

  @Test
  public void testParseDateExpressionToInt_ValidLocalDate() {
    LocalDate localDate = LocalDate.of(2023, 5, 14);
    int dateInt = DateUtils.parseDateExpressionToInt(localDate);
    assertEquals(20230514, dateInt);
  }

  @Test
  public void testParseDateExpressionToInt_NullLocalDate() {
    assertThrows(
        DateTimeParseException.class,
        () -> {
          DateUtils.parseDateExpressionToInt((LocalDate) null);
        });
  }

  @Test
  public void testParseIntToDate() {
    int date = 20230514;
    Date parsedDate = DateUtils.parseIntToDate(date);
    assertEquals(2023 - 1900, parsedDate.getYear());
    assertEquals(4, parsedDate.getMonth()); // Date month is 0-based
    assertEquals(14, parsedDate.getDate());
  }

  @Test
  public void testParseIntToLocalDate() {
    int date = 20230514;
    LocalDate localDate = DateUtils.parseIntToLocalDate(date);
    assertEquals(2023, localDate.getYear());
    assertEquals(5, localDate.getMonthValue());
    assertEquals(14, localDate.getDayOfMonth());
  }

  @Test
  public void testParseIntToLocalDate_InvalidDate() {
    int date = 20231405;
    assertThrows(
        DateTimeParseException.class,
        () -> {
          DateUtils.parseIntToLocalDate(date);
        });
  }
}

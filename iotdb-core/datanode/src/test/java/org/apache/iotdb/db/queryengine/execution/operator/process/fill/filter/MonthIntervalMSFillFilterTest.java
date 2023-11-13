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

package org.apache.iotdb.db.queryengine.execution.operator.process.fill.filter;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MonthIntervalMSFillFilterTest {

  @Test
  public void testMonthIntervalMSFillFilter() {
    MonthIntervalMSFillFilter fillFilter =
        new MonthIntervalMSFillFilter(1, 0, ZoneId.systemDefault());
    DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    LocalDateTime localDateTime = LocalDateTime.parse("2023-02-01T11:47:30", formatter);

    ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(localDateTime);

    Instant instant = localDateTime.toInstant(zoneOffset);

    long previousTime = instant.toEpochMilli();

    assertTrue(
        fillFilter.needFill(
            localDateTime.plusMonths(1).toInstant(zoneOffset).toEpochMilli(), previousTime));
    assertFalse(
        fillFilter.needFill(
            localDateTime.plusMonths(1).toInstant(zoneOffset).toEpochMilli() + 1, previousTime));
  }
}

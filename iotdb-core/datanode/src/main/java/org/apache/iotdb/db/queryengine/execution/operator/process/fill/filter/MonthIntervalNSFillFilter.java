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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;

public class MonthIntervalNSFillFilter extends AbstractMonthIntervalFillFilter {

  public MonthIntervalNSFillFilter(int monthDuration, long nonMonthDuration, ZoneId zone) {
    super(monthDuration, nonMonthDuration, zone);
  }

  @Override
  public boolean needFill(long time, long previousTime) {
    long smaller = Math.min(time, previousTime);
    long greater = Math.max(time, previousTime);
    Instant instant = Instant.ofEpochSecond(smaller / 1_000_000_000L, smaller % 1_000_000_000L);
    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
    Instant upper =
        localDateTime.plusMonths(monthDuration).plusNanos(nonMonthDuration).toInstant(zoneOffset);
    long timeInNs =
        upper.getLong(ChronoField.NANO_OF_SECOND) + upper.getEpochSecond() * 1_000_000_000L;
    return timeInNs >= greater;
  }
}

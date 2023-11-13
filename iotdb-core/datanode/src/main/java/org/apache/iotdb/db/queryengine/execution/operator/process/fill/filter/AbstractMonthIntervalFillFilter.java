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

import org.apache.iotdb.db.queryengine.execution.operator.process.fill.IFillFilter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public abstract class AbstractMonthIntervalFillFilter implements IFillFilter {

  // month part of time duration
  protected final int monthDuration;

  protected final ZoneId zone;

  // non-month part of time duration, its precision is same as current time_precision
  protected final long nonMonthDuration;

  protected final ZoneOffset zoneOffset;

  AbstractMonthIntervalFillFilter(int monthDuration, long nonMonthDuration, ZoneId zone) {
    this.monthDuration = monthDuration;
    this.nonMonthDuration = nonMonthDuration;
    this.zone = zone;
    this.zoneOffset = zone.getRules().getOffset(Instant.now());
  }
}

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

package org.apache.tsfile.read.filter.factory;

import org.apache.tsfile.read.filter.operator.GroupByFilter;
import org.apache.tsfile.read.filter.operator.GroupByMonthFilter;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeBetweenAnd;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeEq;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeGt;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeGtEq;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeIn;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeLt;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeLtEq;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeNotBetweenAnd;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeNotEq;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeNotIn;
import org.apache.tsfile.utils.TimeDuration;

import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class TimeFilterApi {

  private TimeFilterApi() {
    // forbidden construction
  }

  public static TimeGt gt(long value) {
    return new TimeGt(value);
  }

  public static TimeGtEq gtEq(long value) {
    return new TimeGtEq(value);
  }

  public static TimeLt lt(long value) {
    return new TimeLt(value);
  }

  public static TimeLtEq ltEq(long value) {
    return new TimeLtEq(value);
  }

  public static TimeEq eq(long value) {
    return new TimeEq(value);
  }

  public static TimeNotEq notEq(long value) {
    return new TimeNotEq(value);
  }

  public static TimeBetweenAnd between(long value1, long value2) {
    return new TimeBetweenAnd(value1, value2);
  }

  public static TimeNotBetweenAnd notBetween(long value1, long value2) {
    return new TimeNotBetweenAnd(value1, value2);
  }

  public static TimeIn in(Set<Long> values) {
    return new TimeIn(values);
  }

  public static TimeNotIn notIn(Set<Long> values) {
    return new TimeNotIn(values);
  }

  public static GroupByFilter groupBy(
      long startTime, long endTime, long interval, long slidingStep) {
    return new GroupByFilter(startTime, endTime, interval, slidingStep);
  }

  public static GroupByMonthFilter groupByMonth(
      long startTime,
      long endTime,
      TimeDuration interval,
      TimeDuration slidingStep,
      TimeZone timeZone,
      TimeUnit currPrecision) {
    return new GroupByMonthFilter(
        startTime, endTime, interval, slidingStep, timeZone, currPrecision);
  }
}

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

package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.BiConsumer;

public class CommonDateTimeUtils {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CommonDateTimeUtils.class);

  public CommonDateTimeUtils() {
    // Empty constructor
  }

  public static long convertMilliTimeWithPrecision(long milliTime, String timePrecision) {
    long result = milliTime;
    switch (timePrecision) {
      case "ns":
        result = milliTime * 1000_000L;
        break;
      case "us":
        result = milliTime * 1000L;
        break;
      default:
        break;
    }
    // To avoid integer overflow
    if (result < 0) {
      LOGGER.warn(
          "Integer overflow when converting {}ms to {}{}.", milliTime, result, timePrecision);
      result = Long.MAX_VALUE;
    }
    return result;
  }

  public static long convertIoTDBTimeToMillis(long time) {
    String timePrecision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    switch (timePrecision) {
      case "ns":
        return time / 1000_000;
      case "us":
        return time / 1000;
      default:
        return time;
    }
  }

  public static long currentTime() {
    long startupNano = CommonDescriptor.getInstance().getConfig().getStartUpNanosecond();
    String timePrecision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    switch (timePrecision) {
      case "ns":
        return System.currentTimeMillis() * 1000_000 + (System.nanoTime() - startupNano) % 1000_000;
      case "us":
        return System.currentTimeMillis() * 1000 + (System.nanoTime() - startupNano) / 1000 % 1000;
      default:
        return System.currentTimeMillis();
    }
  }

  public static String convertMillisecondToDurationStr(long millisecond) {
    StringBuilder stringBuilder = new StringBuilder();
    boolean minus = false;
    if (millisecond < 0) {
      minus = true;
      millisecond = -millisecond;
      stringBuilder.append("-(");
    }
    Duration duration = Duration.ofMillis(millisecond);
    long days = duration.toDays();
    long years = days / 365;
    days = days % 365;
    long months = days / 30;
    days %= 30;
    long hours = duration.toHours() % 24;
    long minutes = duration.toMinutes() % 60;
    long seconds = duration.getSeconds() % 60;
    long ms = millisecond % 1000;
    BiConsumer<Long, String> append =
        (value, unit) -> {
          if (value > 0) {
            stringBuilder.append(value).append(" ").append(unit).append(" ");
          }
        };
    append.accept(years, "year");
    append.accept(months, "month");
    append.accept(days, "day");
    append.accept(hours, "hour");
    append.accept(minutes, "minute");
    append.accept(seconds, "second");
    append.accept(ms, "ms");
    String result = stringBuilder.toString();
    if (result.endsWith(" ")) {
      result = result.substring(0, result.length() - 1);
    }
    if (minus) {
      result += ")";
    }
    if (result.isEmpty()) {
      result = "0 ms";
    }
    return result;
  }
}

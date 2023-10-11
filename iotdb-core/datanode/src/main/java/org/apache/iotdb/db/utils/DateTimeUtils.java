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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;

import java.util.Calendar;

public class DateTimeUtils {

  private DateTimeUtils() {
    // forbidding instantiation
  }

  /**
   * Convert duration string to time value. CurrentTime is used to calculate the days of natural
   * month. If it's set as -1, which means a context free situation, then '1mo' will be thought as
   * 30 days.
   *
   * @param duration represent duration string like: 12d8m9ns, 1y1mo, etc.
   * @return time in milliseconds, microseconds, or nanoseconds depending on the profile
   */
  public static long convertDurationStrToLong(String duration) {
    return convertDurationStrToLong(-1, duration);
  }

  /** convert duration string to millisecond, microsecond or nanosecond. */
  public static long convertDurationStrToLong(
      long currentTime, long value, String unit, String timestampPrecision) {
    CommonDateTimeUtils.DurationUnit durationUnit = CommonDateTimeUtils.DurationUnit.valueOf(unit);
    long res = value;
    switch (durationUnit) {
      case y:
        res *= 365 * 86_400_000L;
        break;
      case mo:
        if (currentTime == -1) {
          res *= 30 * 86_400_000L;
        } else {
          Calendar calendar = Calendar.getInstance();
          calendar.setTimeZone(SessionManager.getInstance().getSessionTimeZone());
          calendar.setTimeInMillis(currentTime);
          calendar.add(Calendar.MONTH, (int) (value));
          res = calendar.getTimeInMillis() - currentTime;
        }
        break;
      case w:
        res *= 7 * 86_400_000L;
        break;
      case d:
        res *= 86_400_000L;
        break;
      case h:
        res *= 3_600_000L;
        break;
      case m:
        res *= 60_000L;
        break;
      case s:
        res *= 1_000L;
        break;
      default:
        break;
    }

    if ("us".equals(timestampPrecision)) {
      if (unit.equals(CommonDateTimeUtils.DurationUnit.ns.toString())) {
        return value / 1000;
      } else if (unit.equals(CommonDateTimeUtils.DurationUnit.us.toString())) {
        return value;
      } else {
        return res * 1000;
      }
    } else if ("ns".equals(timestampPrecision)) {
      if (unit.equals(CommonDateTimeUtils.DurationUnit.ns.toString())) {
        return value;
      } else if (unit.equals(CommonDateTimeUtils.DurationUnit.us.toString())) {
        return value * 1000;
      } else {
        return res * 1000_000;
      }
    } else {
      if (unit.equals(CommonDateTimeUtils.DurationUnit.ns.toString())) {
        return value / 1000_000;
      } else if (unit.equals(CommonDateTimeUtils.DurationUnit.us.toString())) {
        return value / 1000;
      } else {
        return res;
      }
    }
  }

  @TestOnly
  public static long convertDurationStrToLongForTest(
      long value, String unit, String timestampPrecision) {
    return convertDurationStrToLong(-1, value, unit, timestampPrecision);
  }

  public static long currentTime() {
    long startupNano = IoTDBDescriptor.getInstance().getConfig().getStartUpNanosecond();
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

  /**
   * convert duration string to time value.
   *
   * @param duration represent duration string like: 12d8m9ns, 1y1mo, etc.
   * @return time in milliseconds, microseconds, or nanoseconds depending on the profile
   */
  public static long convertDurationStrToLong(
      long currentTime, String duration, String timestampPrecision) {
    long total = 0;
    long temp = 0;
    for (int i = 0; i < duration.length(); i++) {
      char ch = duration.charAt(i);
      if (Character.isDigit(ch)) {
        temp *= 10;
        temp += (ch - '0');
      } else {
        String unit = String.valueOf(duration.charAt(i));
        // This is to identify units with two letters.
        if (i + 1 < duration.length() && !Character.isDigit(duration.charAt(i + 1))) {
          i++;
          unit += duration.charAt(i);
        }
        total +=
            convertDurationStrToLong(
                currentTime == -1 ? -1 : currentTime + total,
                temp,
                unit.toLowerCase(),
                timestampPrecision);
        temp = 0;
      }
    }
    return total;
  }

  public static long convertDurationStrToLong(long currentTime, String duration) {
    return convertDurationStrToLong(
        currentTime, duration, CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  public static long convertDurationStrToLong(String duration, String timestampPrecision) {
    return convertDurationStrToLong(-1, duration, timestampPrecision);
  }

  /**
   * add natural months based on the startTime to avoid edge cases, ie 2/28
   *
   * @param startTime current start time
   * @param numMonths numMonths is updated in hasNextWithoutConstraint()
   * @return nextStartTime
   */
  public static long calcIntervalByMonth(long startTime, long numMonths) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(SessionManager.getInstance().getSessionTimeZone());
    calendar.setTimeInMillis(startTime);
    boolean isLastDayOfMonth =
        calendar.get(Calendar.DAY_OF_MONTH) == calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
    calendar.add(Calendar.MONTH, (int) (numMonths));
    if (isLastDayOfMonth) {
      calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
    }
    return calendar.getTimeInMillis();
  }
}

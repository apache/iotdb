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

package org.apache.iotdb.postgresql.session;

import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PostgreSQL's {@code statement_timeout}: a number of milliseconds or a number with a unit ({@code
 * us}, {@code ms}, {@code s}, {@code min}, {@code h}, {@code d}); 0 switches it off.
 */
public final class StatementTimeout {

  private static final Pattern VALUE =
      Pattern.compile("(\\d+(?:\\.\\d+)?)\\s*(us|ms|s|min|h|d)?", Pattern.CASE_INSENSITIVE);

  private StatementTimeout() {}

  /**
   * Parses a value.
   *
   * @return {@link Duration#ZERO} for no timeout (0 or no value)
   * @throws IllegalArgumentException if the value is not a valid time
   */
  public static Duration parse(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Duration.ZERO;
    }
    Matcher m = VALUE.matcher(value.trim());
    if (!m.matches()) {
      throw new IllegalArgumentException(
          "invalid value for parameter \"statement_timeout\": \"" + value + "\"");
    }
    double amount = Double.parseDouble(m.group(1));
    String unit = m.group(2) == null ? "ms" : m.group(2).toLowerCase(Locale.ROOT);
    double millis;
    switch (unit) {
      case "us":
        millis = amount / 1000;
        break;
      case "s":
        millis = amount * 1000;
        break;
      case "min":
        millis = amount * 60_000;
        break;
      case "h":
        millis = amount * 3_600_000;
        break;
      case "d":
        millis = amount * 86_400_000;
        break;
      default:
        millis = amount;
    }
    // PostgreSQL rounds to whole milliseconds; a positive value below 1 ms is 1 ms
    long rounded = Math.round(millis);
    return Duration.ofMillis(amount > 0 && rounded == 0 ? 1 : rounded);
  }

  /** The timeout the engine gets: the smaller of its own and the session's (if set). */
  public static long engineTimeoutMillis(long engineTimeout, Duration statementTimeout) {
    if (statementTimeout.isZero()) {
      return engineTimeout;
    }
    long session = statementTimeout.toMillis();
    return engineTimeout <= 0 ? session : Math.min(engineTimeout, session);
  }
}

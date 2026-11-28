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

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Time zone settings read the way PostgreSQL reads them (startup parameter {@code TimeZone}, {@code
 * SET TIME ZONE}). Two forms differ from {@link ZoneId#of}:
 *
 * <ul>
 *   <li>A plain number ({@code 8}, {@code -3.5}) is an offset in hours, positive east of Greenwich
 *       (ISO, like SQL's {@code SET TIME ZONE -8}).
 *   <li>A POSIX zone without daylight saving rules ({@code UTC+8}, {@code GMT-08:00}, {@code
 *       +02:00}, {@code <+03>-03}): positive offsets are <em>west</em> of Greenwich. pgJDBC relies
 *       on this: on a JVM in {@code GMT+08:00} it sends {@code GMT-08:00}.
 * </ul>
 *
 * Everything else is a zone name ({@code Europe/Berlin}, {@code UTC}, {@code EST5EDT}).
 */
public final class PgTimeZones {

  private static final Pattern NUMBER = Pattern.compile("[+-]?(\\d+(\\.\\d*)?|\\.\\d+)");

  /** Optional abbreviation (letters or {@code <...>}), then the POSIX offset. */
  private static final Pattern POSIX =
      Pattern.compile("(?:[A-Za-z]{3,}|<[^<>]+>)?([+-]?)(\\d{1,2})(?::(\\d{2})(?::(\\d{2}))?)?");

  private PgTimeZones() {}

  /**
   * The zone a PostgreSQL time zone setting stands for.
   *
   * @throws DateTimeException if the value is no valid zone
   */
  public static ZoneId parse(String value) {
    String text = value.trim();
    if (NUMBER.matcher(text).matches()) {
      double hours = Double.parseDouble(text);
      if (Math.abs(hours) > 18) {
        throw new DateTimeException("offset out of range: " + text);
      }
      return ZoneOffset.ofTotalSeconds((int) Math.round(hours * 3600));
    }
    Matcher posix = POSIX.matcher(text);
    if (posix.matches()) {
      int seconds =
          Integer.parseInt(posix.group(2)) * 3600
              + (posix.group(3) == null ? 0 : Integer.parseInt(posix.group(3)) * 60)
              + (posix.group(4) == null ? 0 : Integer.parseInt(posix.group(4)));
      // POSIX: positive is west of Greenwich, so the ISO offset has the opposite sign
      return ZoneOffset.ofTotalSeconds("-".equals(posix.group(1)) ? seconds : -seconds);
    }
    return ZoneId.of(text);
  }
}

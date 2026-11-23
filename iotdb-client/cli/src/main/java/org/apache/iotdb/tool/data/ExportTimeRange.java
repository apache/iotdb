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

package org.apache.iotdb.tool.data;

import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.exception.ArgsErrorException;

import java.time.ZoneId;

/** A closed interval, resolved using the connected server's precision and session time zone. */
final class ExportTimeRange {
  private final Long start;
  private final Long end;

  private ExportTimeRange(Long start, Long end) {
    this.start = start;
    this.end = end;
  }

  static ExportTimeRange parse(String start, String end, ZoneId zone, String precision)
      throws ArgsErrorException {
    Long startValue = parseBound(start, zone, precision);
    Long endValue = parseBound(end, zone, precision);
    if (startValue != null && endValue != null && startValue > endValue) {
      throw new ArgsErrorException(
          CliMessages.EXCEPTION_START_TIME_MUST_BE_LESS_THAN_OR_EQUAL_TO_END_TIME_3551199C);
    }
    return new ExportTimeRange(startValue, endValue);
  }

  private static Long parseBound(String value, ZoneId zone, String precision)
      throws ArgsErrorException {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    String trimmed = value.trim();
    try {
      return Long.parseLong(trimmed);
    } catch (NumberFormatException ignored) {
      try {
        return DateTimeUtils.convertDatetimeStrToLong(trimmed, zone, precision);
      } catch (RuntimeException e) {
        throw new ArgsErrorException(
            String.format(
                CliMessages.EXCEPTION_CANNOT_PARSE_TIME_ARG_ARG_40257DF7, value, e.getMessage()));
      }
    }
  }

  String whereClause() {
    if (start == null && end == null) {
      return "";
    }
    StringBuilder sql = new StringBuilder(" WHERE ");
    if (start != null) {
      sql.append("time >= ").append(start);
    }
    if (start != null && end != null) {
      sql.append(" AND ");
    }
    if (end != null) {
      sql.append("time <= ").append(end);
    }
    return sql.toString();
  }
}

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

package org.apache.iotdb.postgresql.types;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.postgresql.protocol.PgException;
import org.apache.iotdb.postgresql.protocol.SqlState;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Turns Bind parameter values into table-model literals: by the parameter's type OID in text or
 * binary format. Timestamps become the epoch value in the database's precision; parameters without
 * a type (OID 0) are recognised from their text (integer, decimal, timestamp, else text), as
 * PostgreSQL would infer them from a comparison with a column of that type.
 */
public final class ParameterDecoder {

  private static final Pattern INTEGER = Pattern.compile("[+-]?\\d{1,18}");
  private static final Pattern DECIMAL =
      Pattern.compile("[+-]?(\\d+\\.\\d*|\\.\\d+|\\d+)([eE][+-]?\\d+)?");
  private static final Pattern TIMESTAMP =
      Pattern.compile("\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}(:\\d{2}(\\.\\d+)?)?.*");

  /** PostgreSQL's text form: {@code 2026-11-19 10:15:30.123456+01}, offset optional. */
  private static final DateTimeFormatter PG_TIMESTAMP =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .optionalStart()
          .appendLiteral(' ')
          .optionalEnd()
          .optionalStart()
          .appendLiteral('T')
          .optionalEnd()
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .optionalStart()
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .optionalEnd()
          .optionalEnd()
          .optionalStart()
          .appendOffset("+HH:mm", "Z")
          .optionalEnd()
          .optionalStart()
          .appendOffset("+HH", "Z")
          .optionalEnd()
          .toFormatter(Locale.ROOT);

  private static final long PG_EPOCH_MICROS = 946_684_800_000_000L;
  private static final int PG_EPOCH_DAYS = 10_957;

  private final ZoneId zone;
  private final TimestampPrecision precision;

  public ParameterDecoder(ZoneId zone, TimestampPrecision precision) {
    this.zone = zone;
    this.precision = precision;
  }

  /**
   * The literal for one parameter value.
   *
   * @param typeOid the type from Parse, 0 if unspecified
   * @param binary whether the value is in binary format
   * @param value the value, {@code null} for SQL NULL
   */
  public Literal decode(int typeOid, boolean binary, byte[] value) throws PgException {
    if (value == null) {
      return new NullLiteral();
    }
    try {
      return binary
          ? binary(typeOid, value)
          : text(typeOid, new String(value, StandardCharsets.UTF_8));
    } catch (RuntimeException e) {
      throw new PgException(
          SqlState.INVALID_TEXT_REPRESENTATION,
          "invalid value for a parameter of type " + typeName(typeOid) + ": " + e.getMessage());
    }
  }

  private Literal text(int oid, String text) {
    switch (oid) {
      case 21:
      case 23:
      case 20:
      case 26:
        return new LongLiteral(text.trim());
      case 700:
      case 701:
      case 1700:
        return new DoubleLiteral(text.trim());
      case 16:
        return new BooleanLiteral(parseBoolean(text) ? "true" : "false");
      case 1184:
      case 1114:
        return new LongLiteral(String.valueOf(timestamp(text, oid == 1114)));
      case 1082:
        return new StringLiteral(LocalDate.parse(text.trim()).toString());
      case 17:
        return new BinaryLiteral(byteaText(text));
      case 0:
      case 705:
        return infer(text);
      default:
        return new StringLiteral(text);
    }
  }

  private Literal infer(String text) {
    String trimmed = text.trim();
    if (INTEGER.matcher(trimmed).matches()) {
      return new LongLiteral(trimmed);
    }
    if (DECIMAL.matcher(trimmed).matches()) {
      return new DoubleLiteral(trimmed);
    }
    if (TIMESTAMP.matcher(trimmed).matches()) {
      try {
        return new LongLiteral(String.valueOf(timestamp(trimmed, false)));
      } catch (DateTimeParseException e) {
        // not a timestamp after all: text
      }
    }
    return new StringLiteral(text);
  }

  private Literal binary(int oid, byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    switch (oid) {
      case 21:
        return new LongLiteral(String.valueOf(buffer.getShort()));
      case 23:
      case 26:
        return new LongLiteral(String.valueOf(buffer.getInt()));
      case 20:
        return new LongLiteral(String.valueOf(buffer.getLong()));
      case 700:
        return new DoubleLiteral(String.valueOf(buffer.getFloat()));
      case 701:
        return new DoubleLiteral(String.valueOf(buffer.getDouble()));
      case 16:
        return new BooleanLiteral(buffer.get() != 0 ? "true" : "false");
      case 1184:
      case 1114:
        return new LongLiteral(
            String.valueOf(precision.fromEpochMicros(buffer.getLong() + PG_EPOCH_MICROS)));
      case 1082:
        return new StringLiteral(
            LocalDate.ofEpochDay(buffer.getInt() + (long) PG_EPOCH_DAYS).toString());
      case 17:
        return new BinaryLiteral(value);
      case 25:
      case 1043:
      case 19:
      case 1042:
      case 0:
      case 705:
        return text(oid == 0 || oid == 705 ? 0 : 25, new String(value, StandardCharsets.UTF_8));
      default:
        throw new IllegalArgumentException("binary format of type " + oid + " is not supported");
    }
  }

  /** A timestamp in the database's precision; without an offset it is in the session's zone. */
  long timestamp(String text, boolean withoutZone) {
    String trimmed = text.trim();
    java.time.temporal.TemporalAccessor parsed = PG_TIMESTAMP.parse(trimmed);
    java.time.Instant instant;
    if (!withoutZone && parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
      instant = OffsetDateTime.from(parsed).toInstant();
    } else {
      instant = LocalDateTime.from(parsed).atZone(zone).toInstant();
    }
    long micros =
        Math.addExact(
            Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1000);
    return precision.fromEpochMicros(micros);
  }

  private static boolean parseBoolean(String text) {
    String t = text.trim().toLowerCase(Locale.ROOT);
    switch (t) {
      case "t":
      case "true":
      case "y":
      case "yes":
      case "on":
      case "1":
        return true;
      case "f":
      case "false":
      case "n":
      case "no":
      case "off":
      case "0":
        return false;
      default:
        throw new IllegalArgumentException("not a boolean: " + text);
    }
  }

  private static byte[] byteaText(String text) {
    if (!text.startsWith("\\x")) {
      throw new IllegalArgumentException("only the hex format of bytea is supported");
    }
    String hex = text.substring(2);
    byte[] bytes = new byte[hex.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }
    return bytes;
  }

  private static String typeName(int oid) {
    return PgType.ofOid(oid).map(PgType::typeName).orElse("oid " + oid);
  }
}

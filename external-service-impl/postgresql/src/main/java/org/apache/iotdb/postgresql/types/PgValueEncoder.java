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

import org.apache.tsfile.block.column.Column;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Encodes table-model values in PostgreSQL's text and binary formats. Null values are not encoded
 * here; the row writer sends them as length -1.
 */
public final class PgValueEncoder {

  /** Days from 1970-01-01 to PostgreSQL's epoch 2000-01-01. */
  static final long PG_EPOCH_DAYS = 10_957L;

  /** Microseconds from 1970-01-01T00:00Z to PostgreSQL's epoch 2000-01-01T00:00Z. */
  static final long PG_EPOCH_MICROS = PG_EPOCH_DAYS * 86_400_000_000L;

  private static final byte[] TRUE = {'t'};
  private static final byte[] FALSE = {'f'};
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private final ZoneId zone;
  private final TimestampPrecision precision;

  /**
   * @param zone the session's time zone, for the text format of timestamps
   * @param precision the table model's timestamp precision
   */
  public PgValueEncoder(ZoneId zone, TimestampPrecision precision) {
    this.zone = zone;
    this.precision = precision;
  }

  /** The value at {@code position} in PostgreSQL's text format (UTF-8). */
  public byte[] text(PgType type, Column column, int position) {
    switch (type) {
      case BOOL:
        return column.getBoolean(position) ? TRUE : FALSE;
      case INT4:
        return ascii(Integer.toString(column.getInt(position)));
      case INT8:
        return ascii(Long.toString(column.getLong(position)));
      case FLOAT4:
        return ascii(float4Text(column.getFloat(position)));
      case FLOAT8:
        return ascii(float8Text(column.getDouble(position)));
      case TEXT:
      case VARCHAR:
        return column.getBinary(position).getValues();
      case BYTEA:
        return ascii(byteaText(column.getBinary(position).getValues()));
      case DATE:
        return ascii(dateOf(column.getInt(position)).toString());
      case TIMESTAMPTZ:
        return ascii(timestamptzText(precision.toEpochMicros(column.getLong(position)), zone));
      default:
        throw new IllegalArgumentException("No text format for " + type);
    }
  }

  /** The value at {@code position} in PostgreSQL's binary format. */
  public byte[] binary(PgType type, Column column, int position) {
    switch (type) {
      case BOOL:
        return new byte[] {(byte) (column.getBoolean(position) ? 1 : 0)};
      case INT4:
        return ByteBuffer.allocate(4).putInt(column.getInt(position)).array();
      case INT8:
        return ByteBuffer.allocate(8).putLong(column.getLong(position)).array();
      case FLOAT4:
        return ByteBuffer.allocate(4).putFloat(column.getFloat(position)).array();
      case FLOAT8:
        return ByteBuffer.allocate(8).putDouble(column.getDouble(position)).array();
      case TEXT:
      case VARCHAR:
      case BYTEA:
        return column.getBinary(position).getValues();
      case DATE:
        return ByteBuffer.allocate(4)
            .putInt((int) (dateOf(column.getInt(position)).toEpochDay() - PG_EPOCH_DAYS))
            .array();
      case TIMESTAMPTZ:
        return ByteBuffer.allocate(8)
            .putLong(precision.toEpochMicros(column.getLong(position)) - PG_EPOCH_MICROS)
            .array();
      default:
        throw new IllegalArgumentException("No binary format for " + type);
    }
  }

  /** A table-model DATE (an int {@code yyyyMMdd}) as a date. */
  static LocalDate dateOf(int yyyyMMdd) {
    return LocalDate.of(yyyyMMdd / 10_000, yyyyMMdd / 100 % 100, yyyyMMdd % 100);
  }

  /** PostgreSQL's ISO output of {@code timestamptz}, e.g. {@code 2026-10-25 01:30:00.5+02}. */
  static String timestamptzText(long epochMicros, ZoneId zone) {
    Instant instant =
        Instant.ofEpochSecond(
            Math.floorDiv(epochMicros, 1_000_000L), Math.floorMod(epochMicros, 1_000_000L) * 1000L);
    OffsetDateTime time = instant.atZone(zone).toOffsetDateTime();
    StringBuilder out = new StringBuilder(32);
    out.append(time.toLocalDate()).append(' ');
    two(out, time.getHour()).append(':');
    two(out, time.getMinute()).append(':');
    two(out, time.getSecond());
    int micros = time.getNano() / 1000;
    if (micros != 0) {
      String fraction = String.format("%06d", micros);
      int end = fraction.length();
      while (fraction.charAt(end - 1) == '0') {
        end--;
      }
      out.append('.').append(fraction, 0, end);
    }
    ZoneOffset offset = time.getOffset();
    int seconds = offset.getTotalSeconds();
    out.append(seconds < 0 ? '-' : '+');
    seconds = Math.abs(seconds);
    two(out, seconds / 3600);
    if (seconds % 3600 != 0) {
      out.append(':');
      two(out, seconds % 3600 / 60);
      if (seconds % 60 != 0) {
        out.append(':');
        two(out, seconds % 60);
      }
    }
    return out.toString();
  }

  /** PostgreSQL's float8 output with the default extra_float_digits (shortest exact). */
  static String float8Text(double value) {
    if (Double.isNaN(value)) {
      return "NaN";
    }
    if (Double.isInfinite(value)) {
      return value > 0 ? "Infinity" : "-Infinity";
    }
    return shortest(Double.toString(value), value == 0.0 && 1 / value < 0, 15);
  }

  /** PostgreSQL's float4 output with the default extra_float_digits (shortest exact). */
  static String float4Text(float value) {
    if (Float.isNaN(value)) {
      return "NaN";
    }
    if (Float.isInfinite(value)) {
      return value > 0 ? "Infinity" : "-Infinity";
    }
    return shortest(Float.toString(value), value == 0.0f && 1 / value < 0, 6);
  }

  /**
   * Plain notation when the decimal exponent is within [-4, digits), else {@code d.ddde+XX}, like
   * PostgreSQL's {@code float8out}/{@code float4out}.
   */
  private static String shortest(String javaText, boolean negativeZero, int digits) {
    if (negativeZero) {
      return "-0";
    }
    BigDecimal decimal = new BigDecimal(javaText).stripTrailingZeros();
    if (decimal.signum() == 0) {
      return "0";
    }
    int exponent = decimal.precision() - decimal.scale() - 1;
    if (exponent >= -4 && exponent < digits) {
      return decimal.toPlainString();
    }
    String unscaled = decimal.unscaledValue().abs().toString();
    StringBuilder out = new StringBuilder();
    if (decimal.signum() < 0) {
      out.append('-');
    }
    out.append(unscaled.charAt(0));
    if (unscaled.length() > 1) {
      out.append('.').append(unscaled, 1, unscaled.length());
    }
    out.append('e').append(exponent < 0 ? '-' : '+');
    two(out, Math.abs(exponent));
    return out.toString();
  }

  /** PostgreSQL's hex output of {@code bytea}: {@code \x} followed by lower-case hex digits. */
  static String byteaText(byte[] value) {
    StringBuilder out = new StringBuilder(2 + value.length * 2).append("\\x");
    for (byte b : value) {
      out.append(HEX[(b >> 4) & 0xF]).append(HEX[b & 0xF]);
    }
    return out.toString();
  }

  private static StringBuilder two(StringBuilder out, int value) {
    if (value < 10) {
      out.append('0');
    }
    return out.append(value);
  }

  private static byte[] ascii(String value) {
    return value.getBytes(StandardCharsets.US_ASCII);
  }
}

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
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PgValueEncoderTest {

  private static final ZoneId BERLIN = ZoneId.of("Europe/Berlin");

  /** One value of one type in a column. */
  private static Column column(TSDataType type, Object value) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(type));
    builder.getTimeColumnBuilder().writeLong(0);
    builder.getColumnBuilder(0).writeObject(value);
    builder.declarePosition();
    TsBlock block = builder.build();
    return block.getColumn(0);
  }

  private static String text(PgType type, TSDataType dataType, Object value, TimestampPrecision p) {
    return new String(
        new PgValueEncoder(BERLIN, p).text(type, column(dataType, value), 0),
        StandardCharsets.UTF_8);
  }

  private static String text(PgType type, TSDataType dataType, Object value) {
    return text(type, dataType, value, TimestampPrecision.MS);
  }

  private static byte[] binary(
      PgType type, TSDataType dataType, Object value, TimestampPrecision p) {
    return new PgValueEncoder(BERLIN, p).binary(type, column(dataType, value), 0);
  }

  @Test
  public void everyExposedTypeHasItsOid() {
    assertEquals(16, PgType.of(TSDataType.BOOLEAN).get().oid());
    assertEquals(23, PgType.of(TSDataType.INT32).get().oid());
    assertEquals(20, PgType.of(TSDataType.INT64).get().oid());
    assertEquals(700, PgType.of(TSDataType.FLOAT).get().oid());
    assertEquals(701, PgType.of(TSDataType.DOUBLE).get().oid());
    assertEquals(25, PgType.of(TSDataType.TEXT).get().oid());
    assertEquals(25, PgType.of(TSDataType.STRING).get().oid());
    assertEquals(17, PgType.of(TSDataType.BLOB).get().oid());
    assertEquals(1082, PgType.of(TSDataType.DATE).get().oid());
    assertEquals(1184, PgType.of(TSDataType.TIMESTAMP).get().oid());
    assertEquals(1185, PgType.TIMESTAMPTZ.arrayOid());
    assertEquals(false, PgType.of(TSDataType.OBJECT).isPresent());
  }

  @Test
  public void simpleTextValues() {
    assertEquals("t", text(PgType.BOOL, TSDataType.BOOLEAN, true));
    assertEquals("f", text(PgType.BOOL, TSDataType.BOOLEAN, false));
    assertEquals("-42", text(PgType.INT4, TSDataType.INT32, -42));
    assertEquals("9007199254740993", text(PgType.INT8, TSDataType.INT64, 9007199254740993L));
    assertEquals(
        "Grüße", text(PgType.TEXT, TSDataType.STRING, new Binary("Grüße", StandardCharsets.UTF_8)));
    assertEquals(
        "\\x00ff10",
        text(PgType.BYTEA, TSDataType.BLOB, new Binary(new byte[] {0, (byte) 0xff, 0x10})));
    assertEquals("2026-10-25", text(PgType.DATE, TSDataType.DATE, 20261025));
  }

  @Test
  public void floatTextFollowsPostgresShortestOutput() {
    assertEquals("1.5", PgValueEncoder.float8Text(1.5));
    assertEquals("1", PgValueEncoder.float8Text(1.0));
    assertEquals("0", PgValueEncoder.float8Text(0.0));
    assertEquals("-0", PgValueEncoder.float8Text(-0.0));
    assertEquals("0.1", PgValueEncoder.float8Text(0.1));
    assertEquals("0.0001", PgValueEncoder.float8Text(0.0001));
    assertEquals("1e-05", PgValueEncoder.float8Text(0.00001));
    assertEquals("123456789012345", PgValueEncoder.float8Text(123456789012345.0));
    assertEquals("1e+15", PgValueEncoder.float8Text(1e15));
    assertEquals("1.7976931348623157e+308", PgValueEncoder.float8Text(Double.MAX_VALUE));
    assertEquals("-2.5e-07", PgValueEncoder.float8Text(-2.5e-7));
    assertEquals("NaN", PgValueEncoder.float8Text(Double.NaN));
    assertEquals("Infinity", PgValueEncoder.float8Text(Double.POSITIVE_INFINITY));
    assertEquals("-Infinity", PgValueEncoder.float8Text(Double.NEGATIVE_INFINITY));
    assertEquals("123456", PgValueEncoder.float4Text(123456f));
    assertEquals("1.234567e+06", PgValueEncoder.float4Text(1234567f));
    assertEquals("0.1", PgValueEncoder.float4Text(0.1f));
    assertEquals("NaN", PgValueEncoder.float4Text(Float.NaN));
  }

  @Test
  public void timestampTextUsesTheSessionZoneLikePostgres() {
    // 2026-10-25 00:30 UTC is still summer time in Berlin, 01:30 UTC already winter time
    assertEquals(
        "2026-10-25 02:30:00+02", text(PgType.TIMESTAMPTZ, TSDataType.TIMESTAMP, 1792888200000L));
    assertEquals(
        "2026-10-25 02:30:00+01", text(PgType.TIMESTAMPTZ, TSDataType.TIMESTAMP, 1792891800000L));
    assertEquals(
        "2026-10-25 02:30:00.5+01", text(PgType.TIMESTAMPTZ, TSDataType.TIMESTAMP, 1792891800500L));
    assertEquals(
        "2026-10-25 02:30:00.000001+01",
        text(PgType.TIMESTAMPTZ, TSDataType.TIMESTAMP, 1792891800000001L, TimestampPrecision.US));
    assertEquals(
        "1969-12-31 23:59:59.999+00", PgValueEncoder.timestamptzText(-1000L, ZoneId.of("UTC")));
    assertEquals(
        "2026-01-01 05:30:00+05:30",
        PgValueEncoder.timestamptzText(1767225600000000L, ZoneId.of("Asia/Kolkata")));
  }

  @Test
  public void binaryValues() {
    assertArrayEquals(
        new byte[] {1}, binary(PgType.BOOL, TSDataType.BOOLEAN, true, TimestampPrecision.MS));
    assertArrayEquals(
        ByteBuffer.allocate(4).putInt(-42).array(),
        binary(PgType.INT4, TSDataType.INT32, -42, TimestampPrecision.MS));
    assertArrayEquals(
        ByteBuffer.allocate(8).putDouble(Double.NaN).array(),
        binary(PgType.FLOAT8, TSDataType.DOUBLE, Double.NaN, TimestampPrecision.MS));
    // 2000-01-01 is day 0, 2026-10-25 is day 9794
    assertArrayEquals(
        ByteBuffer.allocate(4).putInt(9794).array(),
        binary(PgType.DATE, TSDataType.DATE, 20261025, TimestampPrecision.MS));
    assertArrayEquals(
        ByteBuffer.allocate(4).putInt(0).array(),
        binary(PgType.DATE, TSDataType.DATE, 20000101, TimestampPrecision.MS));
  }

  @Test
  public void binaryTimestampsAreMicrosSince2000InEveryPrecision() {
    long micros = 1792891800000001L - PgValueEncoder.PG_EPOCH_MICROS;
    assertArrayEquals(
        ByteBuffer.allocate(8).putLong(micros).array(),
        binary(PgType.TIMESTAMPTZ, TSDataType.TIMESTAMP, 1792891800000001L, TimestampPrecision.US));
    assertArrayEquals(
        ByteBuffer.allocate(8).putLong(micros).array(),
        binary(
            PgType.TIMESTAMPTZ, TSDataType.TIMESTAMP, 1792891800000001999L, TimestampPrecision.NS));
    assertArrayEquals(
        ByteBuffer.allocate(8).putLong(micros - 1).array(),
        binary(PgType.TIMESTAMPTZ, TSDataType.TIMESTAMP, 1792891800000000L, TimestampPrecision.US));
    assertArrayEquals(
        ByteBuffer.allocate(8).putLong(-PgValueEncoder.PG_EPOCH_MICROS).array(),
        binary(PgType.TIMESTAMPTZ, TSDataType.TIMESTAMP, 0L, TimestampPrecision.MS));
  }
}

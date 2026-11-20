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

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Parameters of every mapped type, text and binary, all three precisions. */
public class ParameterDecoderTest {

  private static final ZoneId BERLIN = ZoneId.of("Europe/Berlin");

  /** 2026-10-25T00:00:00Z: microseconds since 2000-01-01 (PostgreSQL's binary epoch). */
  private static final long PG_MICROS = 1792886400_000_000L - 946684800_000_000L;

  private static Literal text(ParameterDecoder d, int oid, String value) throws PgException {
    return d.decode(oid, false, value.getBytes(StandardCharsets.UTF_8));
  }

  private static String longValue(Literal literal) {
    assertTrue(literal.getClass().getSimpleName(), literal instanceof LongLiteral);
    return ((LongLiteral) literal).getValue();
  }

  @Test
  public void textValues() throws Exception {
    ParameterDecoder d = new ParameterDecoder(BERLIN, TimestampPrecision.MS);
    assertEquals("42", longValue(text(d, 23, " 42 ")));
    assertEquals("-7", longValue(text(d, 21, "-7")));
    assertEquals("9000000000", longValue(text(d, 20, "9000000000")));
    assertEquals(2.5, ((DoubleLiteral) text(d, 701, "2.5")).getValue(), 0);
    assertEquals(0.25, ((DoubleLiteral) text(d, 700, "0.25")).getValue(), 0);
    assertTrue(((BooleanLiteral) text(d, 16, "t")).getValue());
    assertTrue(!((BooleanLiteral) text(d, 16, "off")).getValue());
    assertEquals("2026-03-01", ((StringLiteral) text(d, 1082, "2026-03-01")).getValue());
    assertEquals("hello", ((StringLiteral) text(d, 25, "hello")).getValue());
    assertEquals("hello", ((StringLiteral) text(d, 1043, "hello")).getValue());
    assertTrue(text(d, 17, "\\x0102") instanceof BinaryLiteral);
    assertTrue(d.decode(23, false, null) instanceof NullLiteral);
  }

  @Test
  public void timestampsInEveryPrecision() throws Exception {
    long ms = 1792886400_000L;
    String[] precisions = {"ms", "us", "ns"};
    long[] expected = {ms, ms * 1000, ms * 1_000_000};
    for (int i = 0; i < precisions.length; i++) {
      ParameterDecoder d = new ParameterDecoder(BERLIN, TimestampPrecision.of(precisions[i]));
      // timestamptz with an offset
      assertEquals(String.valueOf(expected[i]), longValue(text(d, 1184, "2026-10-25 00:00:00+00")));
      // timestamp: local time in the session zone (CEST here, UTC+2), an offset is ignored
      assertEquals(
          String.valueOf(expected[i] - 7_200_000L * (expected[i] / ms)),
          longValue(text(d, 1114, "2026-10-25 00:00:00+05")));
      // binary: microseconds since 2000-01-01 UTC
      byte[] binary = ByteBuffer.allocate(8).putLong(PG_MICROS).array();
      assertEquals(String.valueOf(expected[i]), longValue(d.decode(1184, true, binary)));
    }
  }

  @Test
  public void binaryValues() throws Exception {
    ParameterDecoder d = new ParameterDecoder(BERLIN, TimestampPrecision.MS);
    assertEquals(
        "7", longValue(d.decode(21, true, ByteBuffer.allocate(2).putShort((short) 7).array())));
    assertEquals("42", longValue(d.decode(23, true, ByteBuffer.allocate(4).putInt(42).array())));
    assertEquals("-1", longValue(d.decode(20, true, ByteBuffer.allocate(8).putLong(-1).array())));
    assertEquals(
        1.5,
        ((DoubleLiteral) d.decode(701, true, ByteBuffer.allocate(8).putDouble(1.5).array()))
            .getValue(),
        0);
    assertEquals(
        0.5,
        ((DoubleLiteral) d.decode(700, true, ByteBuffer.allocate(4).putFloat(0.5f).array()))
            .getValue(),
        0);
    assertTrue(((BooleanLiteral) d.decode(16, true, new byte[] {1})).getValue());
    // dates: days since 2000-01-01
    assertEquals(
        "2000-01-31",
        ((StringLiteral) d.decode(1082, true, ByteBuffer.allocate(4).putInt(30).array()))
            .getValue());
    assertEquals(
        "abc",
        ((StringLiteral) d.decode(1043, true, "abc".getBytes(StandardCharsets.UTF_8))).getValue());
  }

  @Test
  public void untypedParametersAreInferred() throws Exception {
    ParameterDecoder d = new ParameterDecoder(BERLIN, TimestampPrecision.MS);
    assertTrue(text(d, 0, "12") instanceof LongLiteral);
    assertTrue(text(d, 0, "1.5") instanceof DoubleLiteral);
    assertTrue(text(d, 0, "2026-10-25 00:00:00+00") instanceof LongLiteral);
    assertTrue(text(d, 0, "m1") instanceof StringLiteral);
  }

  @Test
  public void invalidValuesAre22P02() {
    ParameterDecoder d = new ParameterDecoder(BERLIN, TimestampPrecision.MS);
    try {
      text(d, 1184, "yesterday");
      fail("invalid");
    } catch (PgException e) {
      assertEquals("22P02", e.sqlState());
    }
  }
}

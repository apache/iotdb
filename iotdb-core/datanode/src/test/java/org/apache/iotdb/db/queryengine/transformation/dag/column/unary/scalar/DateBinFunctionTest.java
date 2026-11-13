/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.calc.transformation.dag.column.leaf.NullColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.TimeZone;

import static org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer.dateBin;
import static org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer.nextDateBin;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DateBinFunctionTest {

  private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private static final long MILLIS_IN_SECOND = 1000;
  private static final long MILLIS_IN_MINUTE = 60 * MILLIS_IN_SECOND;
  private static final long MILLIS_IN_HOUR = 60 * MILLIS_IN_MINUTE;
  private static final long MILLIS_IN_DAY = 24 * MILLIS_IN_HOUR;

  private final ZoneId zoneId = ZoneId.of("UTC+0");

  @Before
  public void before() {
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public long getTimestamp(String time) throws ParseException {
    return format.parse(time).getTime();
  }

  @Test
  public void testWithoutOrigin() throws ParseException {
    long result = dateBin(getTimestamp("2000-01-01 00:00:00.000"), 0, 0, MILLIS_IN_DAY, zoneId);
    assertEquals(getTimestamp("2000-01-01 00:00:00.000"), result);
  }

  @Test
  public void testOrigin() throws ParseException {
    long result =
        dateBin(
            getTimestamp("2000-01-01 01:30:00.000"),
            getTimestamp("2000-01-01 01:00:00.000"),
            0,
            MILLIS_IN_HOUR,
            zoneId);
    assertEquals(getTimestamp("2000-01-01 01:00:00.000"), result);
  }

  @Test
  public void testMonthInterval() throws ParseException {
    long result =
        dateBin(
            getTimestamp("2000-01-01 00:30:00.000"),
            getTimestamp("2000-01-01 00:00:00.000"),
            1,
            0,
            zoneId);
    assertEquals(getTimestamp("2000-01-01 00:00:00.000"), result);
  }

  @Test
  public void testOriginGtSource() throws ParseException {
    long result =
        dateBin(
            getTimestamp("2000-01-01 00:00:00.000"),
            getTimestamp("2000-05-01 00:00:00.000"),
            1,
            0,
            zoneId);
    assertEquals(getTimestamp("2000-01-01 00:00:00.000"), result);
  }

  @Test
  public void testOriginBeforeUnixEpoch() throws ParseException {
    long result =
        dateBin(
            getTimestamp("2000-01-01 00:00:00.000"),
            getTimestamp("1969-12-31 00:00:00.000"),
            0,
            MILLIS_IN_DAY,
            zoneId);
    assertEquals(getTimestamp("2000-01-01 00:00:00.000"), result);
  }

  @Test
  public void testZeroInterval() throws ParseException {
    long result =
        dateBin(
            getTimestamp("2000-01-01 00:00:00.000"),
            getTimestamp("2000-01-01 00:00:00.000"),
            0,
            0,
            zoneId);
    assertEquals(getTimestamp("2000-01-01 00:00:00.000"), result);
  }

  @Test
  public void testSourceBeforeUnixEpoch() throws ParseException {
    long result =
        dateBin(
            getTimestamp("1969-12-31 23:00:00.000"),
            getTimestamp("2000-01-01 00:00:00.000"),
            0,
            MILLIS_IN_DAY,
            zoneId);
    assertEquals(getTimestamp("1969-12-31 00:00:00.000"), result);
  }

  @Test
  public void testMonthBinKeepsFractionalOrigin() throws ParseException {
    long origin = 500L;
    long oneMonthLater = getTimestamp("1970-02-01 00:00:00.500");
    long justBeforeNextMonth = oneMonthLater - 1;

    assertEquals(origin, dateBin(origin, origin, 1, 0, zoneId));
    assertEquals(origin, dateBin(justBeforeNextMonth, origin, 1, 0, zoneId));
    assertEquals(oneMonthLater, dateBin(oneMonthLater, origin, 1, 0, zoneId));

    long marchOrigin = getTimestamp("2024-03-15 00:00:00.500");
    long aprilOrigin = getTimestamp("2024-04-15 00:00:00.500");
    assertEquals(marchOrigin, dateBin(marchOrigin, marchOrigin, 1, 0, zoneId));
    assertEquals(aprilOrigin, dateBin(aprilOrigin, marchOrigin, 1, 0, zoneId));

    long exactSecond = getTimestamp("2024-03-15 00:00:00.000");
    long nextExactMonth = getTimestamp("2024-04-15 00:00:00.000");
    assertEquals(exactSecond, dateBin(exactSecond, exactSecond, 1, 0, zoneId));
    assertEquals(nextExactMonth, dateBin(nextExactMonth, exactSecond, 1, 0, zoneId));
  }

  @Test
  public void testMonthBinStartEndAndGapFillKeepFractionalMillis() throws ParseException {
    long origin = 500L;
    DateBinFunctionColumnTransformer transformer =
        new DateBinFunctionColumnTransformer(
            TIMESTAMP, 1, 0, new NullColumnTransformer(), origin, zoneId);

    long oneMonthLater = getTimestamp("1970-02-01 00:00:00.500");
    assertArrayEquals(new long[] {origin, oneMonthLater}, transformer.dateBinStartEnd(origin));
    assertEquals(oneMonthLater, nextDateBin(1, zoneId, origin));
  }

  @Test
  public void testMonthBinKeepsSubMillisecondOriginUnderUsAndNs() {
    String originalPrecision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    try {
      // 1.5 ms after the epoch, expressed in microseconds and nanoseconds.
      CommonDescriptor.getInstance().getConfig().setTimestampPrecision("us");
      assertEquals(1500L, dateBin(1500L, 1500L, 1, 0, zoneId));
      assertEquals(1500L, nextDateBin(1, zoneId, 1500L) - 2678400000000L);

      CommonDescriptor.getInstance().getConfig().setTimestampPrecision("ns");
      assertEquals(1_500_000L, dateBin(1_500_000L, 1_500_000L, 1, 0, zoneId));
    } finally {
      CommonDescriptor.getInstance().getConfig().setTimestampPrecision(originalPrecision);
    }
  }
}

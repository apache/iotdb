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

import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.apache.tsfile.read.common.type.TimestampType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.TimeZone;

import static org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer.dateBin;
import static org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer.nextDateBin;
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
  public void testMonthIntervalWithSubSecondOrigin() throws ParseException {
    // 500 ms is 1970-01-01T00:00:00.500Z, 2678400500 ms is also exactly one month later
    assertEquals(500, dateBin(500, 500, 1, 0, zoneId));
    assertEquals(2678400500L, dateBin(2678400500L, 500, 1, 0, zoneId));
    // a point 200 ms into the next bin must still map to the start of that bin
    assertEquals(2678400500L, dateBin(2678400700L, 500, 1, 0, zoneId));
    // an origin far from the epoch must keep its sub-second part too
    long origin = getTimestamp("2024-03-15 00:00:00.500");
    assertEquals(origin, dateBin(origin, origin, 1, 0, zoneId));
    assertEquals(origin, dateBin(origin + MILLIS_IN_DAY, origin, 1, 0, zoneId));
  }

  @Test
  public void testMonthIntervalStartEndWithSubSecondOrigin() {
    DateBinFunctionColumnTransformer transformer =
        new DateBinFunctionColumnTransformer(TimestampType.TIMESTAMP, 1, 0, null, 500, zoneId);
    assertArrayEquals(
        new long[] {2678400500L, 5097600500L}, transformer.dateBinStartEnd(2678400700L));
  }

  @Test
  public void testNextDateBinWithSubSecondOrigin() {
    assertEquals(2678400500L, nextDateBin(1, zoneId, 500));
  }

  @Test
  public void testMonthIntervalWithSubSecondOriginInMicrosecondPrecision() {
    setTimestampPrecision("us");
    assertEquals(1500, dateBin(1500, 1500, 1, 0, zoneId));
    // 2678400000000 us is one month after the epoch
    assertEquals(2678400001500L, dateBin(2678400001500L, 1500, 1, 0, zoneId));
  }

  @Test
  public void testMonthIntervalWithSubSecondOriginInNanosecondPrecision() {
    setTimestampPrecision("ns");
    assertEquals(1_500_000L, dateBin(1_500_000L, 1_500_000L, 1, 0, zoneId));
  }

  private static void setTimestampPrecision(String precision) {
    CommonDescriptor.getInstance().getConfig().setTimestampPrecision(precision);
  }

  @After
  public void after() {
    setTimestampPrecision("ms");
  }
}

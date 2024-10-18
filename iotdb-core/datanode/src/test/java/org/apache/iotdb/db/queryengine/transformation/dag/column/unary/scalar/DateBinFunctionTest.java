/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.TimeZone;

import static org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer.dateBin;
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
}

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

import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;
import static org.junit.Assert.assertEquals;

public class DateBinFunctionTest {

  private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private static DateBinFunctionColumnTransformer transformer;

  private static final long MILLIS_IN_SECOND = 1000;
  private static final long MILLIS_IN_MINUTE = 60 * MILLIS_IN_SECOND;
  private static final long MILLIS_IN_HOUR = 60 * MILLIS_IN_MINUTE;
  private static final long MILLIS_IN_DAY = 24 * MILLIS_IN_HOUR;

  @Before
  public void before() {
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public long getTimestamp(String time) throws ParseException {
    return format.parse(time).getTime();
  }

  public DateBinFunctionColumnTransformer getTransformer(
      long monthDuration, long nonMonthDuration, long origin) {
    return new DateBinFunctionColumnTransformer(
        TIMESTAMP, monthDuration, nonMonthDuration, null, origin, ZoneId.of("UTC+0"));
  }

  @Test
  public void testWithoutOrigin() throws ParseException {
    transformer = getTransformer(0, MILLIS_IN_DAY, 0);
    long result = transformer.dateBin(getTimestamp("2000-01-01 00:00:00.000"));
    assertEquals(getTimestamp("2000-01-01 00:00:00.000"), result);
  }

  @Test
  public void testOrigin() throws ParseException {
    transformer = getTransformer(0, MILLIS_IN_HOUR, getTimestamp("2000-01-01 01:00:00.000"));
    long result = transformer.dateBin(getTimestamp("2000-01-01 01:30:00.000"));
    assertEquals(getTimestamp("2000-01-01 01:00:00.000"), result);
  }

  @Test
  public void testMonthInterval() throws ParseException {
    transformer = getTransformer(1, 0, getTimestamp("2000-01-01 00:00:00.000"));
    long result = transformer.dateBin(getTimestamp("2000-01-01 00:30:00.000"));
    assertEquals(getTimestamp("2000-01-01 00:00:00.000"), result);
  }

  @Test
  public void testOriginGtSource() throws ParseException {
    transformer = getTransformer(1, 0, getTimestamp("2000-05-01 00:00:00.000"));
    long result = transformer.dateBin(getTimestamp("2000-01-01 00:00:00.000"));
    assertEquals(getTimestamp("2000-01-01 00:00:00.000"), result);
  }

  @Test
  public void testOriginBeforeUnixEpoch() throws ParseException {
    transformer = getTransformer(0, MILLIS_IN_DAY, getTimestamp("1969-12-31 00:00:00.000"));
    long result = transformer.dateBin(getTimestamp("2000-01-01 00:00:00.000"));
    assertEquals(getTimestamp("2000-01-01 00:00:00.000"), result);
  }

  @Test
  public void testZeroInterval() throws ParseException {
    transformer = getTransformer(0, 0, getTimestamp("2000-01-01 00:00:00.000"));
    long result = transformer.dateBin(getTimestamp("2000-01-01 00:00:00.000"));
    assertEquals(getTimestamp("2000-01-01 00:00:00.000"), result);
  }

  @Test
  public void testSourceBeforeUnixEpoch() throws ParseException {
    transformer = getTransformer(0, MILLIS_IN_DAY, getTimestamp("2000-01-01 00:00:00.000"));
    long result = transformer.dateBin(getTimestamp("1969-12-31 23:00:00.000"));
    assertEquals(getTimestamp("1969-12-31 00:00:00.000"), result);
  }
}

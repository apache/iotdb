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
package org.apache.iotdb.tsfile.utils;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.tsfile.utils.TimeDuration.calcPositiveIntervalByMonth;

public class TimeDurationTest {
  @Test
  public void calculateIntervalTest() {
    // 1mo duration after 2023-01-31
    long result =
        calcPositiveIntervalByMonth(
            Timestamp.valueOf("2023-01-31 00:00:00").getTime(),
            new TimeDuration(1, 0),
            TimeZone.getDefault(),
            TimeUnit.MILLISECONDS);
    Assert.assertEquals(Timestamp.valueOf("2023-02-28 00:00:00").getTime(), result);
    result =
        calcPositiveIntervalByMonth(
            Timestamp.valueOf("2023-02-28 00:00:00").getTime(),
            new TimeDuration(2, 0),
            TimeZone.getDefault(),
            TimeUnit.MILLISECONDS);
    Assert.assertEquals(Timestamp.valueOf("2023-04-28 00:00:00").getTime(), result);
    result =
        calcPositiveIntervalByMonth(
            Timestamp.valueOf("2023-01-30 00:00:00").getTime(),
            new TimeDuration(2, 0),
            TimeZone.getDefault(),
            TimeUnit.MILLISECONDS);
    Assert.assertEquals(Timestamp.valueOf("2023-03-30 00:00:00").getTime(), result);
    // 1mo1d duration after 2023-01-31
    result =
        calcPositiveIntervalByMonth(
            Timestamp.valueOf("2023-01-31 00:00:00").getTime(),
            new TimeDuration(1, 86400_000),
            TimeZone.getDefault(),
            TimeUnit.MILLISECONDS);
    Assert.assertEquals(Timestamp.valueOf("2023-03-01 00:00:00").getTime(), result);

    // 1mo1d1ns duration after 2023-01-31
    result =
        calcPositiveIntervalByMonth(
            Timestamp.valueOf("2023-01-31 00:00:00").getTime() * 1000_000,
            new TimeDuration(1, 86400_000_000_001L),
            TimeZone.getDefault(),
            TimeUnit.NANOSECONDS);
    Assert.assertEquals(Timestamp.valueOf("2023-03-01 00:00:00").getTime() * 1000_000 + 1, result);
  }
}

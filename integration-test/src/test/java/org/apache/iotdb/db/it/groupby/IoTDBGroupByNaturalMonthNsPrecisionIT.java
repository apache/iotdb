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
package org.apache.iotdb.db.it.groupby;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Calendar;
import java.util.TimeZone;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.count;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGroupByNaturalMonthNsPrecisionIT extends IoTDBGroupByNaturalMonthIT {
  static {
    for (long i = 1604102400000L /*  2020-10-31 00:00:00 */;
        i <= 1617148800000L /* 2021-03-31 00:00:00 */;
        i += 86400_000L) {
      dataSet.add(
          "insert into root.sg1.d1(timestamp, temperature) values (" + i * 1000000 + ", 1)");
    }

    // TimeRange: [2023-01-01 00:00:00, 2027-01-01 00:00:00]
    // insert a record each first day of month
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(TimeZone.getTimeZone("+00:00"));
    calendar.setTimeInMillis(1672531200000L);
    for (long i = calendar.getTimeInMillis();
        i <= 1798761600000L;
        calendar.add(Calendar.MONTH, 1), i = calendar.getTimeInMillis()) {
      dataSet.add("insert into root.test.d1(timestamp, s1) values (" + i * 1000000 + ", 1)");
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    df.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimestampPrecision("ns");
    EnvFactory.getEnv().initClusterEnvironment();
    currPrecision = EnvFactory.getEnv().getConfig().getCommonConfig().getTimestampPrecision();
    prepareData(dataSet.toArray(new String[0]));
  }

  @Test
  public void groupByNaturalMonthFailTest() {
    assertTestFail(
        "select sum(temperature) from root.sg1.d1 "
            + "GROUP BY ([2021-01-31, 2021-03-31), 1mo) order by time desc",
        "doesn't support order by time desc now.");

    // 2970-01-01 in ns precision has exceeded field of long number
    assertTestFail(
        "select sum(temperature) from root.sg1.d1 GROUP BY ([1970-01-01, 2970-01-01), 40d, 1mo)",
        "Input time format 2970-01-01 error.");
  }

  @Test
  public void groupByNaturalMonthWithMixedUnit2() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.test.d1.s1")};
    String[] retArray =
        new String[] {
          // [01-28, 03-01 + 1ns)
          "1674864000000000000,2,",
          // [03-01 + 1ns, 03-30 + 2ns)
          "1677628800000000001,0,",
          // [03-30 + 2ns, 05-01 + 3ns)
          "1680134400000000002,2,",
          // [05-01 + 3ns, 05-29 + 4ns)
          "1682899200000000003,0,"
        };
    // the part in timeDuration finer than current time precision will be discarded
    resultSetEqualTest(
        "select count(s1) from root.test.d1 " + "group by ([2023-01-28, 2023-05-29), 1mo1d1ns)",
        expectedHeader,
        retArray,
        null,
        currPrecision);
  }
}

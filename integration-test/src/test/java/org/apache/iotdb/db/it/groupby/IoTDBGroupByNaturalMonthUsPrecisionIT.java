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
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Calendar;
import java.util.TimeZone;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGroupByNaturalMonthUsPrecisionIT extends IoTDBGroupByNaturalMonthIT {
  static {
    for (long i = 1604102400000L /*  2020-10-31 00:00:00 */;
        i <= 1617148800000L /* 2021-03-31 00:00:00 */;
        i += 86400_000L) {
      dataSet.add("insert into root.sg1.d1(timestamp, temperature) values (" + i * 1000 + ", 1)");
    }

    // TimeRange: [2023-01-01 00:00:00, 2027-01-01 00:00:00]
    // insert a record each first day of month
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(TimeZone.getTimeZone("+00:00"));
    calendar.setTimeInMillis(1672531200000L);
    for (long i = calendar.getTimeInMillis();
        i <= 1798761600000L;
        calendar.add(Calendar.MONTH, 1), i = calendar.getTimeInMillis()) {
      dataSet.add("insert into root.test.d1(timestamp, s1) values (" + i * 1000 + ", 1)");
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    df.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimestampPrecision("us");
    EnvFactory.getEnv().initClusterEnvironment();
    currPrecision = EnvFactory.getEnv().getConfig().getCommonConfig().getTimestampPrecision();
    prepareData(dataSet.toArray(new String[0]));
  }
}

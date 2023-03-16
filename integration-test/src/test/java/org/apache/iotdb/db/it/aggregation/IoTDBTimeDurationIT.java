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

package org.apache.iotdb.db.it.aggregation;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBTimeDurationIT {
  // 2 devices 4 regions
  protected static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.db",
        "CREATE DATABASE root.db1",
        "CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN tags(city=Beijing)",
        "CREATE TIMESERIES root.db.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db.d2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db.d2.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db1.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db1.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db1.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db1.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db1.d2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db1.d2.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN tags(city=Nanjing)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(1, 2, 10, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(2, null, 20, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(3, 10, 0, null)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(4, 303, 30, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(5, null, 20, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(6, 110, 20, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(7, 302, 20, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(8, 110, null, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(9, 60, 20, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(10,70, 20, null)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(1677570934, 30, 0, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(1, 80, 30, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(2, null, 30, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(3, 60, 30, null)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(4, 40, 20, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(5, null, 40, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(6, 40, 50, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(7, 40, 60, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(8, 40, null, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(9, 50, 70, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(10, 60, 80, null)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(1677570934, 90, 90, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(1, 70, 90, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(2, null, 80, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(3, 80, 70, null)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(4, 90, 70, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(5, null, 60, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(6, 20, 100, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(7, 10, 20, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(8, 20, null, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(9, 30, 20, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(10, 10, 20, null)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(1677570934, 0, 20, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(1, 10, 20, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(2, null, null, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(3, null, 20, null)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(4, 0, 0, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(5, null, 0, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(6, 0, 0, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(7, 1, 0, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(8, 1, null, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(9, null, 20, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(10, 1, 0, null)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(1677570939, 1, 0, true)",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTimeDurationSingleColumnOnOneDataRegion() {
    // normal
    String[] expectedHeader = new String[] {"time_duration(root.db.d1.s1)"};
    String[] retArray = new String[] {"8,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1 where time < 10", expectedHeader, retArray);

    // order by time desc
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1  where time < 10 order by time desc",
        expectedHeader,
        retArray);

    // order by time asc
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1  where time < 10 order by time asc",
        expectedHeader,
        retArray);

    // limit
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1  where time < 10 limit 3",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {"Device", "time_duration(s1)"};
    retArray = new String[] {"root.db.d1,8,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1  where time < 10 align by device",
        expectedHeader,
        retArray);

    // align by time
    expectedHeader = new String[] {"time_duration(root.db.d1.s1)"};
    retArray = new String[] {"8,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1  where time < 10 align by time",
        expectedHeader,
        retArray);

    // group by level=1(database)
    expectedHeader = new String[] {"time_duration(root.db1.*.s1),time_duration(root.db.*.s1)"};
    retArray = new String[] {"8,8,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.** where time < 10 group by level=1",
        expectedHeader,
        retArray);

    // group by level=2(device)
    expectedHeader = new String[] {"time_duration(root.*.d1.s1)", "time_duration(root.*.d2.s1)"};
    retArray = new String[] {"8,8,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.** where time < 10 group by level=2",
        expectedHeader,
        retArray);

    // group by session
    expectedHeader = new String[] {"Time", "time_duration(root.db1.d1.s1)"};
    retArray = new String[] {"3,6,"};
    // ignore = true
    resultSetEqualTest(
        "select time_duration(s1) from root.db1.d1 where time<10 group by session(100ms)",
        expectedHeader,
        retArray);

    // group by variation
    expectedHeader = new String[] {"Time", "time_duration(root.db1.d2.s1)"};
    retArray = new String[] {"1,0,", "3,null,", "4,0,", "6,0,", "7,0,", "8,0,", "9,null,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.db1.d2 where time<10 group by variation(root.db.d1.s1)",
        expectedHeader,
        retArray);

    // group by condition
    expectedHeader = new String[] {"Time", "time_duration(root.db1.d2.s1)"};
    retArray = new String[] {"1,9,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.db1.d2 group by condition(root.db.d2.s1 > 10,KEEP>=1,ignoreNull=true)",
        expectedHeader,
        retArray);

    // group by tags
    expectedHeader = new String[] {"city", "time_duration(s1)"};
    retArray = new String[] {"Nanjing,1677570938,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.db1.d2 group by tags(city)", expectedHeader, retArray);

    // group by time
    expectedHeader = new String[] {"Time", "time_duration(root.db1.d2.s1)"};
    retArray = new String[] {"1,3,", "6,2,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.db1.d2 group by time([1,10),5ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void testTimeDurationManyColumnOnManyDataRegion() {
    // normal
    String[] expectedHeader =
        new String[] {
          "time_duration(root.db1.d1.s1),time_duration(root.db1.d2.s1),time_duration(root.db.d1.s1),time_duration(root.db.d2.s1),time_duration(root.db1.d1.s2),time_duration(root.db1.d2.s2),time_duration(root.db.d1.s2),time_duration(root.db.d2.s2)"
        };
    String[] retArray =
        new String[] {
          "1677570931,1677570938,1677570933,1677570933,1677570932,1677570938,1677570933,1677570933,"
        };
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.**", expectedHeader, retArray);

    // order by time desc
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** order by time desc",
        expectedHeader,
        retArray);

    // order by time asc
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** order by time asc",
        expectedHeader,
        retArray);

    // limit
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** limit 3",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {"Device", "time_duration(s1)", "time_duration(s2)"};
    retArray =
        new String[] {
          "root.db.d1,1677570933,1677570933,",
          "root.db.d2,1677570933,1677570933,",
          "root.db1.d1,1677570931,1677570932,",
          "root.db1.d2,1677570938,1677570938,"
        };
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** align by device",
        expectedHeader,
        retArray);

    // align by time
    expectedHeader =
        new String[] {
          "time_duration(root.db1.d1.s1),time_duration(root.db1.d2.s1),time_duration(root.db.d1.s1),time_duration(root.db.d2.s1),time_duration(root.db1.d1.s2),time_duration(root.db1.d2.s2),time_duration(root.db.d1.s2),time_duration(root.db.d2.s2)"
        };
    retArray =
        new String[] {
          "1677570931,1677570938,1677570933,1677570933,1677570932,1677570938,1677570933,1677570933,"
        };
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** align by time",
        expectedHeader,
        retArray);

    // group by level=1(database)
    expectedHeader =
        new String[] {
          "time_duration(root.db1.*.s1),time_duration(root.db.*.s1),time_duration(root.db1.*.s2),time_duration(root.db.*.s2)"
        };
    retArray = new String[] {"1677570938,1677570933,1677570938,1677570933,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** group by level=1",
        expectedHeader,
        retArray);

    // group by level=2(device)
    expectedHeader =
        new String[] {
          "time_duration(root.*.d1.s1),time_duration(root.*.d2.s1),time_duration(root.*.d1.s2),time_duration(root.*.d2.s2)"
        };
    retArray = new String[] {"1677570933,1677570938,1677570933,1677570938,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** group by level=2",
        expectedHeader,
        retArray);

    // group by session
    expectedHeader =
        new String[] {
          "Time,time_duration(root.db1.d1.s1),time_duration(root.db1.d2.s1),time_duration(root.db.d1.s1),time_duration(root.db.d2.s1),time_duration(root.db1.d1.s2),time_duration(root.db1.d2.s2),time_duration(root.db.d1.s2),time_duration(root.db.d2.s2)"
        };
    retArray = new String[] {"1,7,9,9,9,8,9,9,9,", "1677570934,0,0,0,0,0,0,0,0,"};
    // ignore = true
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** group by session(100ms)",
        expectedHeader,
        retArray);

    // group by variation
    expectedHeader =
        new String[] {
          "Time,time_duration(root.db1.d1.s1),time_duration(root.db1.d2.s1),time_duration(root.db.d1.s1),time_duration(root.db.d2.s1),time_duration(root.db1.d1.s2),time_duration(root.db1.d2.s2),time_duration(root.db.d1.s2),time_duration(root.db.d2.s2)"
        };
    retArray =
        new String[] {
          "1,null,0,0,0,null,0,0,0,",
          "3,0,null,0,0,0,0,0,0,",
          "4,0,0,0,0,0,0,0,0,",
          "6,0,0,0,0,0,0,0,0,",
          "7,0,0,0,0,0,0,0,0,",
          "8,0,0,0,0,null,null,null,null,",
          "9,0,null,0,0,0,0,0,0,",
          "10,0,0,0,0,0,0,0,0,",
          "1677570934,0,null,0,0,0,null,0,0,"
        };
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** group by variation(root.db.d1.s1)",
        expectedHeader,
        retArray);

    // group by condition
    expectedHeader =
        new String[] {
          "Time,time_duration(root.db1.d1.s1),time_duration(root.db1.d2.s1),time_duration(root.db.d1.s1),time_duration(root.db.d2.s1),time_duration(root.db1.d1.s2),time_duration(root.db1.d2.s2),time_duration(root.db.d1.s2),time_duration(root.db.d2.s2)"
        };
    retArray =
        new String[] {"1,1677570931,9,1677570933,1677570933,1677570931,9,1677570933,1677570933,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** group by condition(root.db.d2.s1 > 10,KEEP>=1,ignoreNull=true)",
        expectedHeader,
        retArray);

    // group by tags
    expectedHeader = new String[] {"city", "time_duration(s1)"};
    retArray = new String[] {"Beijing,1677570933,", "Nanjing,1677570938,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.** group by tags(city)", expectedHeader, retArray);

    // group by time
    expectedHeader =
        new String[] {
          "Time,time_duration(root.db1.d1.s1),time_duration(root.db1.d2.s1),time_duration(root.db.d1.s1),time_duration(root.db.d2.s1),time_duration(root.db1.d1.s2),time_duration(root.db1.d2.s2),time_duration(root.db.d1.s2),time_duration(root.db.d2.s2)"
        };
    retArray = new String[] {"1,1,3,3,3,3,4,4,4,", "6,3,2,3,3,3,3,3,3,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** group by time([1,10),5ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void testTimeDurationWithSlidingWindow() {
    assertTestFail(
        "select time_duration(s2) from root.db1.d2 group by ([1,10),5ms,2ms)",
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()
            + ": TIME_DURATION with slidingWindow is not supported now");
  }
}

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
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(1, 0, 0, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(2, null, 0, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(3, 0, 0, null)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(4, 0, 0, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(5, null, 0, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(6, 0, 0, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(7, 0, 0, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(8, 0, null, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(9, 0, 0, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(10, 0, 0, null)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(1677570934, 0, 0, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(1, 0, 0, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(2, null, 0, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(3, 0, 0, null)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(4, 0, 0, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(5, null, 0, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(6, 0, 0, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(7, 0, 0, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(8, 0, null, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(9, 0, 0, true)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(10, 0, 0, null)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3) values(1677570934, 0, 0, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(1, 0, 0, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(2, null, 0, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(3, 0, 0, null)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(4, 0, 0, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(5, null, 0, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(6, 0, 0, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(7, 0, 0, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(8, 0, null, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(9, 0, 0, true)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(10, 0, 0, null)",
        "INSERT INTO root.db1.d1(timestamp,s1,s2,s3) values(1677570934, 0, 0, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(1, 0, 0, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(2, null, null, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(3, null, 0, null)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(4, 0, 0, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(5, null, 0, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(6, 0, 0, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(7, 0, 0, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(8, 0, null, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(9, null, 0, true)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(10, 0, 0, null)",
        "INSERT INTO root.db1.d2(timestamp,s1,s2,s3) values(1677570939, 0, 0, true)",
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
    // single column
    String[] expectedHeader = new String[] {"time_duration(root.db.d1.s1)"};
    String[] retArray = new String[] {"8,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1 where time < 10", expectedHeader, retArray);

    // single column order by time
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1  where time < 10 order by time desc",
        expectedHeader,
        retArray);

    // single column limit
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1  where time < 10 limit 3",
        expectedHeader,
        retArray);

    // single column align by device
    expectedHeader = new String[] {"Device", "time_duration(s1)"};
    retArray = new String[] {"root.db.d1,8,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1  where time < 10 align by device",
        expectedHeader,
        retArray);

    // single column group by level=1(database)
    expectedHeader = new String[] {"time_duration(root.*.d1.s1)", "time_duration(root.*.d2.s1)"};
    retArray = new String[] {"8,8,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.** where time < 10 group by level=2",
        expectedHeader,
        retArray);

    // single column group by level=2(device)
    expectedHeader = new String[] {"time_duration(root.*.d1.s1)", "time_duration(root.*.d2.s1)"};
    retArray = new String[] {"8,8,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.** where time < 10 group by level=2",
        expectedHeader,
        retArray);
  }

  @Test
  public void testTimeDurationSingleColumnOnManyDataRegion() {
    // single column
    String[] expectedHeader = new String[] {"time_duration(root.db.d1.s1)"};
    String[] retArray = new String[] {"1677570933,"};
    resultSetEqualTest("select time_duration(s1) from root.db.d1", expectedHeader, retArray);

    // single column order by time
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1 order by time desc", expectedHeader, retArray);

    // single column limit
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1 limit 3", expectedHeader, retArray);

    // single column align by device
    expectedHeader = new String[] {"Device", "time_duration(s1)"};
    retArray = new String[] {"root.db.d1,1677570933,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.db.d1 align by device", expectedHeader, retArray);

    // single column group by level=1(database)
    expectedHeader = new String[] {"time_duration(root.db1.*.s1)", "time_duration(root.db.*.s1)"};
    retArray = new String[] {"1677570938,1677570933,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.** group by level=1", expectedHeader, retArray);

    // single column group by level=2(device)
    expectedHeader = new String[] {"time_duration(root.*.d1.s1)", "time_duration(root.*.d2.s1)"};
    retArray = new String[] {"1677570933,1677570938,"};
    resultSetEqualTest(
        "select time_duration(s1) from root.** group by level=2", expectedHeader, retArray);
  }

  @Test
  public void testTimeDurationManyColumnOnOneDataRegion() {
    // many column
    String[] expectedHeader =
        new String[] {"time_duration(root.db.d1.s1)", "time_duration(root.db.d1.s2)"};
    String[] retArray = new String[] {"8,8,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.db.d1 where time < 10",
        expectedHeader,
        retArray);

    // many column order by time
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.db.d1  where time < 10 order by time desc",
        expectedHeader,
        retArray);

    // many column limit
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.db.d1  where time < 10 limit 3",
        expectedHeader,
        retArray);

    // many column align by device
    expectedHeader = new String[] {"Device", "time_duration(s1),time_duration(s2)"};
    retArray = new String[] {"root.db.d1,8,8,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.db.d1  where time < 10 align by device",
        expectedHeader,
        retArray);

    // many column group by level=1(database)
    expectedHeader =
        new String[] {
          "time_duration(root.db1.*.s1)",
          "time_duration(root.db.*.s1)",
          "time_duration(root.db1.*.s2)",
          "time_duration(root.db.*.s2)"
        };
    retArray = new String[] {"8,8,8,8,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** where time < 10 group by level=1",
        expectedHeader,
        retArray);

    // many column group by level=2(device)
    expectedHeader =
        new String[] {
          "time_duration(root.*.d1.s1)",
          "time_duration(root.*.d2.s1)",
          "time_duration(root.*.d1.s2)",
          "time_duration(root.*.d2.s2)"
        };
    retArray = new String[] {"8,8,8,8,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** where time < 10 group by level=2",
        expectedHeader,
        retArray);
  }

  @Test
  public void testTimeDurationManyColumnOnManyDataRegion() {
    // many column
    String[] expectedHeader =
        new String[] {"time_duration(root.db.d1.s1)", "time_duration(root.db.d1.s2)"};
    String[] retArray = new String[] {"1677570933,1677570933,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.db.d1", expectedHeader, retArray);

    // many column order by time
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.db.d1 order by time desc",
        expectedHeader,
        retArray);

    // many column limit
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.db.d1 limit 3",
        expectedHeader,
        retArray);

    // many column align by device
    expectedHeader = new String[] {"Device", "time_duration(s1)", "time_duration(s2)"};
    retArray = new String[] {"root.db.d1,1677570933,1677570933,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.db.d1 align by device",
        expectedHeader,
        retArray);

    // many column group by level=1(database)
    expectedHeader =
        new String[] {
          "time_duration(root.db1.*.s1)",
          "time_duration(root.db.*.s1)",
          "time_duration(root.db1.*.s2)",
          "time_duration(root.db.*.s2)"
        };
    retArray = new String[] {"1677570938,1677570933,1677570938,1677570933,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** group by level=1",
        expectedHeader,
        retArray);

    // many column group by level=2(device)
    expectedHeader =
        new String[] {
          "time_duration(root.*.d1.s1)",
          "time_duration(root.*.d2.s1)",
          "time_duration(root.*.d1.s2)",
          "time_duration(root.*.d2.s2)"
        };
    retArray = new String[] {"1677570933,1677570938,1677570933,1677570938,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2) from root.** group by level=2",
        expectedHeader,
        retArray);
  }

  @Test
  public void testTimeDurationOnManyDataRegion() {
    // many columns
    String[] expectedHeader =
        new String[] {
          "time_duration(root.db.d1.s1)",
          "time_duration(root.db.d1.s2)",
          "time_duration(root.db.d1.s3)"
        };
    String[] retArray = new String[] {"1677570933,1677570933,1677570933,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2),time_duration(s3) from root.db.d1",
        expectedHeader,
        retArray);

    // many columns order by time
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2),time_duration(s3) from root.db.d1 order by time desc",
        expectedHeader,
        retArray);

    // many column align by device
    expectedHeader =
        new String[] {"Device", "time_duration(s1)", "time_duration(s2)", "time_duration(s3)"};
    retArray = new String[] {"root.db.d1,1677570933,1677570933,1677570933,"};
    resultSetEqualTest(
        "select time_duration(s1),time_duration(s2),time_duration(s3) from root.db.d1 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testTimeDurationWithWildcard() {
    assertTestFail(
        "select time_duration(*) as res from root.** align by device",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": alias 'res' can only be matched with one time series");
  }

  @Test
  public void testTimeDurationWithSingleColumn() {
    // single column with alias
    String[] expectedHeader = new String[] {"res"};
    String[] retArray = new String[] {"1677570933,"};
    resultSetEqualTest("select time_duration(s1) as res from root.db.d1", expectedHeader, retArray);
  }

  @Test
  public void testTimeDurationGroupByTag() {
    String[] expectedHeader = new String[] {"city", "time_duration(s2)"};
    String[] retArray = new String[] {"Nanjing,8,"};
    resultSetEqualTest(
        "select time_duration(s2) from root.** where time < 10 group by tags(city)",
        expectedHeader,
        retArray);
  }

  @Test
  public void testTimeDurationWithManyColumns() {
    // many column with alias
    String[] expectedHeader = new String[] {"res1", "res2", "res3"};
    String[] retArray = new String[] {"1677570933,1677570933,1677570933,"};
    resultSetEqualTest(
        "select time_duration(s1) as res1,time_duration(s2) as res2,time_duration(s3) as res3 from root.db.d1",
        expectedHeader,
        retArray);
  }
}

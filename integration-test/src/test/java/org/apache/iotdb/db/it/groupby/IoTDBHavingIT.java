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

package org.apache.iotdb.db.it.groupby;

import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.constant.TestConstant.firstValue;
import static org.apache.iotdb.db.constant.TestConstant.lastValue;
import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualWithDescOrderTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.count;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class}) // TODO After old StandAlone remove
public class IoTDBHavingIT {
  private static final String[] SQLs =
      new String[] {
        "SET STORAGE GROUP TO root.test",
        "CREATE TIMESERIES root.test.sg1.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s4 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s4 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE ALIGNED TIMESERIES root.test.sg3(s5 INT32, s6 BOOLEAN, s7 DOUBLE, s8 INT32)",
        "CREATE TIMESERIES root.test.sg5.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg5.s9 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) " + "values(1, true, 1, 1.0, 1)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) " + "values(1, false, 1, 1.0, 1)",
        "INSERT INTO root.test.sg1(timestamp, s2) " + "values(2, 2)",
        "INSERT INTO root.test.sg1(timestamp, s3) " + "values(2, 2.0)",
        "INSERT INTO root.test.sg1(timestamp, s4) " + "values(2, 2)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) " + "values(2, true, 2, 2.0, 2)",
        "flush",
        "INSERT INTO root.test.sg1(timestamp, s1) " + "values(3, false)",
        "INSERT INTO root.test.sg1(timestamp, s2) " + "values(5, 5)",
        "INSERT INTO root.test.sg1(timestamp, s3) " + "values(5, 5.0)",
        "INSERT INTO root.test.sg1(timestamp, s4) " + "values(5, 5)",
        "INSERT INTO root.test.sg2(timestamp, s2) " + "values(5, 5)",
        "INSERT INTO root.test.sg2(timestamp, s3) " + "values(5, 5.0)",
        "INSERT INTO root.test.sg2(timestamp, s4) " + "values(5, 5)",
        "flush",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) " + "values(6, true, 6, 6.0, 6)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) " + "values(6, true, 6, 6.0, 6)",
        "INSERT INTO root.test.sg1(timestamp, s1) " + "values(7, true)",
        "INSERT INTO root.test.sg1(timestamp, s3) " + "values(7, 7.0)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3) " + "values(7, true, 7, 7.0)",
        "flush",
        "INSERT INTO root.test.sg1(timestamp, s1) " + "values(8, true)",
        "INSERT INTO root.test.sg1(timestamp, s2) " + "values(8, 8)",
        "INSERT INTO root.test.sg1(timestamp, s3) " + "values(8, 8.0)",
        "INSERT INTO root.test.sg2(timestamp, s3) " + "values(8, 8.0)",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) " + "values(9, false, 9, 9.0, 9)",
        "INSERT INTO root.test.sg2(timestamp, s1) " + "values(9, true)",
        "flush",
        "INSERT INTO root.test.sg2(timestamp, s2) " + "values(9, 9)",
        "INSERT INTO root.test.sg2(timestamp, s4) " + "values(9, 9)",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) " + "values(10, true, 10, 10.0, 10)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) " + "values(10, true, 10, 10.0, 10)",
        "flush",
        "INSERT INTO root.test.sg3(time, s5, s6, s7, s8) aligned values(1, 1, true, 1.0, 1)",
        "INSERT INTO root.test.sg3(time, s6, s7, s8) aligned values(2, false, 2.0, 2)",
        "INSERT INTO root.test.sg3(time, s5, s7, s8) aligned values(3, 3, 3.0, 3)",
        "INSERT INTO root.test.sg3(time, s5) aligned values(5, 5)",
        "INSERT INTO root.test.sg3(time, s5, s8) aligned values(6, 6, 6)",
        "INSERT INTO root.test.sg3(time, s6) aligned values(7, true)",
        "INSERT INTO root.test.sg3(time, s5, s7) aligned values(8, 8, 8.0)",
        "INSERT INTO root.test.sg3(time, s5, s7, s8) aligned values(9, 9, 9.0, 9)",
        "INSERT INTO root.test.sg3(time, s7, s8) aligned values(10, 10.0, 10)",
        "INSERT INTO root.test.sg5(timestamp, s1, s9) " + "values(1, true, 1)",
        "flush",
      };

  private static long prevPartitionInterval;

  @BeforeClass
  public static void setUp() throws Exception {
    prevPartitionInterval = ConfigFactory.getConfig().getPartitionInterval();
    ConfigFactory.getConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initBeforeClass();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig().setPartitionInterval(prevPartitionInterval);
  }

  @Test
  public void testUnsatisfiedRuleQuery() {
    assertTestFail(
        "select count(s1) from root.** group by ([1,3),1ms), level=1 having sum(d1.s1) > 1",
        "416: Having: when used with GroupByLevel, all paths in expression of Having and Select should have only one node");
    assertTestFail(
        "select count(d1.s1) from root.** group by ([1,3),1ms), level=1 having sum(s1) > 1",
        "416: Having: when used with GroupByLevel, all paths in expression of Having and Select should have only one node");
    assertTestFail(
        "select count(d1.s1) from root.** group by ([1,3),1ms), level=1 having sum(s1) + s1 > 1",
        "416: Raw data and aggregation result hybrid calculation is not supported");
    assertTestFail(
        "select count(d1.s1) from root.** group by ([1,3),1ms), level=1 having s1 + 1 > 1",
        "416: Expression of HAVING clause must to be an Aggregation");
  }

  @Test
  public void groupByTimeWithHavingTest() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.test.sg1.s1")};
    String[] retArray =
        new String[] {
          "2,5.5,4.4,",
          "6,null,null,",
          "10,10.1,10.1,",
          "14,null,null,",
          "18,20.2,20.2,",
          "22,null,null,",
          "26,null,null,"
        };
    resultSetEqualWithDescOrderTest(
        "select count(sg1.s1) from root.test.**"
            + "GROUP BY ([1,11), 1ms)"
            + "Having count(sg1.s2) > 1",
        expectedHeader,
        retArray);
  }

  @Test
  public void groupByLevelWithHavingTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          lastValue("root.ln.wf01.wt01.temperature"),
          firstValue("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "2,5.5,4.4,",
          "6,null,null,",
          "10,10.1,10.1,",
          "14,null,null,",
          "18,20.2,20.2,",
          "22,null,null,",
          "26,null,null,"
        };
    resultSetEqualWithDescOrderTest(
        "select last_value(temperature), first_value(temperature) from root.ln.wf01.wt01 where time > 3 "
            + "GROUP BY ([2,30), 4ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void groupByTimeLevelWithHavingTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          lastValue("root.ln.wf01.wt01.temperature"),
          firstValue("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "2,5.5,4.4,",
          "6,null,null,",
          "10,10.1,10.1,",
          "14,null,null,",
          "18,20.2,20.2,",
          "22,null,null,",
          "26,null,null,"
        };
    resultSetEqualWithDescOrderTest(
        "select last_value(temperature), first_value(temperature) from root.ln.wf01.wt01 where time > 3 "
            + "GROUP BY ([2,30), 4ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void allClauseTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          lastValue("root.ln.wf01.wt01.temperature"),
          firstValue("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "2,5.5,4.4,",
          "6,null,null,",
          "10,10.1,10.1,",
          "14,null,null,",
          "18,20.2,20.2,",
          "22,null,null,",
          "26,null,null,"
        };
    resultSetEqualWithDescOrderTest(
        "select last_value(temperature), first_value(temperature) from root.ln.wf01.wt01 where time > 3 "
            + "GROUP BY ([2,30), 4ms)",
        expectedHeader,
        retArray);
  }
}

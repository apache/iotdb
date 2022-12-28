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
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualWithDescOrderTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.avg;
import static org.apache.iotdb.itbase.constant.TestConstant.count;
import static org.apache.iotdb.itbase.constant.TestConstant.lastValue;
import static org.apache.iotdb.itbase.constant.TestConstant.sum;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBHavingIT {
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.test",
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
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) values(1, true, 1, 1.0, 1)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) values(1, false, 1, 1.0, 1)",
        "INSERT INTO root.test.sg1(timestamp, s2) values(2, 2)",
        "INSERT INTO root.test.sg1(timestamp, s3) values(2, 2.0)",
        "INSERT INTO root.test.sg1(timestamp, s4) values(2, 2)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) values(2, true, 2, 2.0, 2)",
        "flush",
        "INSERT INTO root.test.sg1(timestamp, s1) values(3, false)",
        "INSERT INTO root.test.sg1(timestamp, s2) values(5, 5)",
        "INSERT INTO root.test.sg1(timestamp, s3) values(5, 5.0)",
        "INSERT INTO root.test.sg1(timestamp, s4) values(5, 5)",
        "INSERT INTO root.test.sg2(timestamp, s2) values(5, 5)",
        "INSERT INTO root.test.sg2(timestamp, s3) values(5, 5.0)",
        "INSERT INTO root.test.sg2(timestamp, s4) values(5, 5)",
        "flush",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) values(6, true, 6, 6.0, 6)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) values(6, true, 6, 6.0, 6)",
        "INSERT INTO root.test.sg1(timestamp, s1) values(7, true)",
        "INSERT INTO root.test.sg1(timestamp, s3) values(7, 7.0)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3) values(7, true, 7, 7.0)",
        "flush",
        "INSERT INTO root.test.sg1(timestamp, s1) values(8, true)",
        "INSERT INTO root.test.sg1(timestamp, s2) values(8, 8)",
        "INSERT INTO root.test.sg1(timestamp, s3) values(8, 8.0)",
        "INSERT INTO root.test.sg2(timestamp, s3) values(8, 8.0)",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) values(9, false, 9, 9.0, 9)",
        "INSERT INTO root.test.sg2(timestamp, s1) values(9, true)",
        "flush",
        "INSERT INTO root.test.sg2(timestamp, s2) values(9, 9)",
        "INSERT INTO root.test.sg2(timestamp, s4) values(9, 9)",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) values(10, true, 10, 10.0, 10)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) values(10, true, 10, 10.0, 10)",
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
        "INSERT INTO root.test.sg5(timestamp, s1, s9) values(1, true, 1)",
        "flush",
        "CREATE TIMESERIES root.test1.d1.code TEXT",
        "CREATE TIMESERIES root.test1.d1.tem INT32",
        "INSERT INTO root.test1.d1(timestamp, code, tem) values(1, '123', 345);",
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
  public void testUnsatisfiedRuleQuery() {
    assertTestFail(
        "select count(s1) from root.** group by ([1,3),1ms), level=1 having sum(d1.s1) > 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": When Having used with GroupByLevel: the suffix paths can only be measurement or one-level wildcard");

    assertTestFail(
        "select count(d1.s1) from root.** group by ([1,3),1ms), level=1 having sum(s1) > 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": When Having used with GroupByLevel: the suffix paths can only be measurement or one-level wildcard");

    assertTestFail(
        "select count(d1.s1) from root.** group by ([1,3),1ms), level=1 having sum(s1) + s1 > 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Raw data and aggregation result hybrid calculation is not supported");

    assertTestFail(
        "select count(d1.s1) from root.** group by ([1,3),1ms), level=1 having s1 + 1 > 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Expression of HAVING clause must to be an Aggregation");
  }

  @Test
  public void groupByTimeWithHavingTest() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.test.sg1.s1")};
    String[] retArray = new String[] {"1,1,", "5,1,", "9,2,"};
    resultSetEqualWithDescOrderTest(
        "select count(sg1.s1) from root.** "
            + "GROUP BY ([1,11), 2ms) "
            + "Having count(sg1.s2) > 1",
        expectedHeader,
        retArray);

    resultSetEqualWithDescOrderTest(
        "select count(sg1.s1) from root.**"
            + "GROUP BY ([1,11), 2ms) "
            + "Having count(sg1.s2) > 2",
        expectedHeader,
        new String[] {});
  }

  @Test
  public void groupByTimeAlignByDeviceWithHavingTest() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "Device", count("s1"), count("s2")};
    String[] retArray =
        new String[] {
          "1,root.test.sg1,1,2,",
          "5,root.test.sg1,1,2,",
          "9,root.test.sg1,2,2,",
          "1,root.test.sg2,2,2,",
          "5,root.test.sg2,1,2,",
          "9,root.test.sg2,2,2,",
        };
    resultSetEqualTest(
        "select count(s1), count(s2) from root.** "
            + "GROUP BY ([1,11), 2ms) "
            + "Having count(s2) > 1 "
            + "Align by device",
        expectedHeader,
        retArray);

    resultSetEqualTest(
        "select count(s1), count(s2) from root.** "
            + "GROUP BY ([1,11), 2ms) "
            + "Having count(s2) > 2 "
            + "Align by device",
        expectedHeader,
        new String[] {});
  }

  @Test
  public void groupByLevelWithHavingTest() {
    String[] expectedHeader =
        new String[] {count("root.test.*.s1"), count("root.test.*.s2"), sum("root.test.*.s3")};
    String[] retArray = new String[] {"14,14,87.0,"};
    resultSetEqualWithDescOrderTest(
        "select count(s1), count(s2), sum(s3) from root.** "
            + "GROUP BY level=1 "
            + "Having sum(s3) > 80",
        expectedHeader,
        retArray);

    resultSetEqualWithDescOrderTest(
        "select count(s1), count(s2), sum(s3) from root.** "
            + "GROUP BY level=1 "
            + "Having sum(s3) > 90",
        expectedHeader,
        new String[] {});
  }

  @Test
  public void groupByTimeLevelWithHavingTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, count("root.test.*.s1"), count("root.test.*.s2"),
        };
    String[] retArray =
        new String[] {
          "1,4,4,", "5,2,4,", "9,4,4,",
        };
    resultSetEqualWithDescOrderTest(
        "select count(s1), count(s2) from root.** "
            + "GROUP BY ([1,11),2ms), level=1 "
            + "Having count(s2)>2",
        expectedHeader,
        retArray);

    resultSetEqualWithDescOrderTest(
        "select count(s1), count(s2) from root.** "
            + "GROUP BY ([1,11),2ms), level=1 "
            + "Having count(s2)>4",
        expectedHeader,
        new String[] {});
  }

  @Test
  public void groupByTimeLevelWithHavingLimitTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, count("root.test.*.s1"), count("root.test.*.s2"),
        };
    String[] retArray = new String[] {"5,2,4,"};
    resultSetEqualTest(
        "select count(s1), count(s2) from root.** "
            + "GROUP BY ([1,11),2ms), level=1 "
            + "Having count(s2) > 1 "
            + "Limit 1 offset 1",
        expectedHeader,
        retArray);
  }

  @Test
  public void sameConstantTest() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, avg("root.test1.d1.tem")};
    String[] retArray = new String[] {"1,345.0,"};
    resultSetEqualTest(
        "select avg(tem) from root.test1.d1 "
            + "where code='123' group by([0,5), 1ms) "
            + "having min_value(tem)!=123",
        expectedHeader,
        retArray);
  }

  @Test
  public void caseSensitiveHavingTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          lastValue("root.test.sg5.s1"),
          lastValue("root.test.sg1.s1"),
          lastValue("root.test.sg2.s1"),
        };
    String[] retArray =
        new String[] {
          "1,true,true,true,", "5,null,true,true,", "7,null,true,true,", "9,null,true,true,"
        };
    resultSetEqualTest(
        "select last_value(s1) from root.** "
            + "GROUP BY ([1,11),2ms) "
            + "Having LAST_VALUE(s2) > 0 ",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {TIMESTAMP_STR, lastValue("root.test.*.s1")};
    retArray = new String[] {"1,true,", "5,true,", "7,true,", "9,true,"};
    resultSetEqualTest(
        "select last_value(s1) from root.** "
            + "GROUP BY ([1,11),2ms), level=1 "
            + "Having LAST_VALUE(s2) > 0 ",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {TIMESTAMP_STR, "Device", lastValue("s1")};
    retArray =
        new String[] {
          "1,root.test.sg1,true,",
          "5,root.test.sg1,true,",
          "7,root.test.sg1,true,",
          "9,root.test.sg1,true,",
          "1,root.test.sg2,true,",
          "5,root.test.sg2,true,",
          "7,root.test.sg2,true,",
          "9,root.test.sg2,true,"
        };
    resultSetEqualTest(
        "select last_value(s1) from root.** "
            + "GROUP BY ([1,11),2ms) "
            + "Having LAST_VALUE(s2) > 0 "
            + "align by device",
        expectedHeader,
        retArray);
  }
}

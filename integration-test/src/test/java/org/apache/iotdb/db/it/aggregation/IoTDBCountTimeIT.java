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
import static org.apache.iotdb.db.queryengine.plan.expression.visitor.CountTimeAggregationAmountVisitor.COUNT_TIME_ONLY_SUPPORT_ONE_WILDCARD;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCountTimeIT {

  protected static final String[] SQL_LIST =
      new String[] {
        // test normal query
        "CREATE DATABASE root.db;",
        "CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.db.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.db.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.db.d2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "INSERT INTO root.db.d1(time, s1) VALUES(1, 1);",
        "INSERT INTO root.db.d1(time, s2) VALUES(2, 2);",
        "INSERT INTO root.db.d2(time, s2) VALUES(1, 1);",
        // test group by time
        "CREATE DATABASE root.downsampling;",
        "CREATE TIMESERIES root.downsampling.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.downsampling.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.downsampling.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.downsampling.d2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "INSERT INTO root.downsampling.d1(time, s1) VALUES(0, 0), (4,4), (5,5), (8,8);",
        "INSERT INTO root.downsampling.d1(time, s2) VALUES(1, 1), (2,2), (5,5), (7,7), (8,8), (9,9);",
        "INSERT INTO root.downsampling.d2(time, s1) VALUES(1, 1), (2,2), (5,5), (7,7), (8,8);",
        "INSERT INTO root.downsampling.d2(time, s2) VALUES(0, 0), (4,4), (5,5), (8,8);",
        // test group by variation
        "CREATE DATABASE root.variation;",
        "CREATE TIMESERIES root.variation.d1.state WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.variation.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.variation.d2.state WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.variation.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "INSERT INTO root.variation.d1(time, state) VALUES(0,0), (1,0), (3,0), (4,0),(5,1),(6,1);",
        "INSERT INTO root.variation.d1(time, s1) VALUES(0,0), (2,2), (3,3), (6,6);",
        "INSERT INTO root.variation.d2(time, state) VALUES(0,0), (2,1), (3,1), (4,1), (6,1);",
        "INSERT INTO root.variation.d2(time, s1) VALUES(1,1), (2,2), (3,3);",
        // test group by session
        "CREATE DATABASE root.session;",
        "CREATE TIMESERIES root.session.d1.state WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.session.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.session.d2.state WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.session.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "INSERT INTO root.session.d1(time, state) VALUES(0,0), (1,0), (20,0), (23,0),(40,0),(55,1),(56,1);",
        "INSERT INTO root.session.d1(time, s1) VALUES(0,0), (20,2), (23,3), (56,6);",
        "INSERT INTO root.session.d2(time, state) VALUES(0,0), (20,1), (23,1), (40,1), (56,1);",
        "INSERT INTO root.session.d2(time, s1) VALUES(1,1), (20,2), (23,3);",
        // test group by condition
        "CREATE DATABASE root.condition;",
        "CREATE TIMESERIES root.condition.d1.state WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.condition.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.condition.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.condition.d2.state WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.condition.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "INSERT INTO root.condition.d1(time, state) VALUES(0,0), (1,1), (23,1),(40,0),(55,1),(56,1);",
        "INSERT INTO root.condition.d1(time, s1) VALUES(0,0), (23,3), (56,6);",
        "INSERT INTO root.condition.d1(time, s2) VALUES(0,0), (1,1), (20,2), (23,3);",
        "INSERT INTO root.condition.d2(time, state) VALUES(0,0), (20,1), (23,1), (40,1), (56,1);",
        "INSERT INTO root.condition.d2(time, s1) VALUES(1,1), (20,2), (23,3);",
        // test having
        "CREATE DATABASE root.having;",
        "CREATE TIMESERIES root.having.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.having.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.having.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.having.d2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "INSERT INTO root.having.d1(time, s1) VALUES(0, 0), (4,4), (5,5), (8,8);",
        "INSERT INTO root.having.d1(time, s2) VALUES(1, 1), (2,2), (5,5), (7,7), (8,8), (9,9);",
        "INSERT INTO root.having.d2(time, s1) VALUES(1, 1), (2,2), (5,5), (7,7), (8,8), (9,9);",
        "INSERT INTO root.having.d2(time, s2) VALUES(0, 0), (4,4), (5,5), (8,8);",
        "FLUSH;"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    // TODO set
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQL_LIST);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void normalQueryTest() {
    // align by time
    String[] expectedHeader = new String[] {"count_time(*)"};
    String[] retArray = new String[] {"2,"};
    resultSetEqualTest("SELECT COUNT_TIME(*) FROM root.db.**;", expectedHeader, retArray);

    expectedHeader = new String[] {"count_time(*)"};
    retArray = new String[] {"2,"};
    resultSetEqualTest(
        "SELECT COUNT_TIME(*) FROM root.db.d1, root.db.d2;", expectedHeader, retArray);

    // align by device
    expectedHeader = new String[] {"Device,count_time(*)"};
    retArray = new String[] {"root.db.d1,2,", "root.db.d2,1,"};
    resultSetEqualTest(
        "select count_time(*) from root.db.** align by device;", expectedHeader, retArray);

    expectedHeader = new String[] {"Device,count_time(*)"};
    retArray = new String[] {"root.db.d1,2,", "root.db.d2,1,"};
    resultSetEqualTest(
        "select count_time(*) from root.db.d1,root.db.d2 align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void groupByTimeTest() {
    // align by time
    String[] expectedHeader = new String[] {"Time,count_time(*)"};
    String[] retArray = new String[] {"0,2,", "2,1,", "4,2,", "6,1,", "8,2,"};
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.downsampling.** GROUP BY([0, 10), 2ms);",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.downsampling.d1,root.downsampling.d2 GROUP BY([0, 10), 2ms);",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.downsampling.d2,root.downsampling.d1 GROUP BY([0, 10), 2ms);",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Time,count_time(*)"};
    retArray = new String[] {"0,2,", "2,1,", "4,2,", "6,1,", "8,2,"};
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.downsampling.d1 GROUP BY([0, 10), 2ms);",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {"Time,Device,count_time(*)"};
    retArray =
        new String[] {
          "0,root.downsampling.d1,2,",
          "2,root.downsampling.d1,1,",
          "4,root.downsampling.d1,2,",
          "6,root.downsampling.d1,1,",
          "8,root.downsampling.d1,2,",
          "0,root.downsampling.d2,2,",
          "2,root.downsampling.d2,1,",
          "4,root.downsampling.d2,2,",
          "6,root.downsampling.d2,1,",
          "8,root.downsampling.d2,1,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.downsampling.** GROUP BY([0, 10), 2ms) ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.downsampling.d1,root.downsampling.d2 GROUP BY([0, 10), 2ms) ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.downsampling.d2,root.downsampling.d1 GROUP BY([0, 10), 2ms) ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // test sort
    expectedHeader = new String[] {"Time,Device,count_time(*)"};
    retArray =
        new String[] {
          "2,root.downsampling.d1,1,",
          "6,root.downsampling.d1,1,",
          "2,root.downsampling.d2,1,",
          "6,root.downsampling.d2,1,",
          "8,root.downsampling.d2,1,",
          "0,root.downsampling.d1,2,",
          "4,root.downsampling.d1,2,",
          "8,root.downsampling.d1,2,",
          "0,root.downsampling.d2,2,",
          "4,root.downsampling.d2,2,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.downsampling.** GROUP BY([0, 10), 2ms) ORDER BY count_time(*) ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void groupByVariationTest() {
    // align by time
    String[] expectedHeader = new String[] {"Time,__endTime,count_time(*)"};
    String[] retArray = new String[] {"0,1,2,", "2,2,1,", "3,4,2,", "5,6,2,"};
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.variation.d1 group by variation(state, 0, ignoreNull=False);",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {"Time,Device,__endTime,count_time(*)"};
    retArray =
        new String[] {
          "0,root.variation.d1,1,2,",
          "2,root.variation.d1,2,1,",
          "3,root.variation.d1,4,2,",
          "5,root.variation.d1,6,2,",
          "0,root.variation.d2,0,1,",
          "1,root.variation.d2,1,1,",
          "2,root.variation.d2,6,4,",
        };
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.variation.** "
            + "group by variation(state, 0, ignoreNull=False) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.variation.d1,root.variation.d2 "
            + "group by variation(state, 0, ignoreNull=False) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.variation.d2,root.variation.d1 "
            + "group by variation(state, 0, ignoreNull=False) align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void groupBySessionTest() {
    // align by time
    String[] expectedHeader = new String[] {"Time,__endTime,count_time(*)"};
    String[] retArray = new String[] {"0,1,2,", "20,23,2,", "40,40,1,", "55,56,2,"};
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.session.** group by session(10ms);",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.session.d1,root.session.d2 group by session(10ms);",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.session.d2,root.session.d1 group by session(10ms);",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {"Time,Device,__endTime,count_time(*)"};
    retArray =
        new String[] {
          "0,root.session.d1,1,2,",
          "20,root.session.d1,23,2,",
          "40,root.session.d1,40,1,",
          "55,root.session.d1,56,2,",
          "0,root.session.d2,1,2,",
          "20,root.session.d2,23,2,",
          "40,root.session.d2,40,1,",
          "56,root.session.d2,56,1,",
        };
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.session.** group by session(10ms) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.session.d1,root.session.d2 group by session(10ms) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.session.d2,root.session.d1 group by session(10ms) align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void groupByConditionTest() {
    // align by time
    String[] expectedHeader = new String[] {"Time,count_time(*)"};
    String[] retArray = new String[] {"55,2,"};
    resultSetEqualTest(
        "select count_time(*) from root.condition.d1 group by condition(state=1, KEEP>=2, ignoreNull=false);",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {"Time,Device,count_time(*)"};
    retArray = new String[] {"55,root.condition.d1,2,", "20,root.condition.d2,4,"};
    resultSetEqualTest(
        "select count_time(*) from root.condition.** group by condition(state=1, KEEP>=2, ignoreNull=false) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select count_time(*) from root.condition.d1,root.condition.d2 group by condition(state=1, KEEP>=2, ignoreNull=false) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select count_time(*) from root.condition.d2,root.condition.d1 group by condition(state=1, KEEP>=2, ignoreNull=false) align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void testUnSupportedSql() {
    assertTestFail(
        "SELECT COUNT_TIME(s1) FROM root.db.**;",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode() + ": " + COUNT_TIME_ONLY_SUPPORT_ONE_WILDCARD);
  }
}

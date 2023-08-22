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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCountTimeAlignedDeviceIT {

  protected static final String[] SQL_LIST =
      new String[] {
        // test normal query
        "CREATE DATABASE root.aligned;",
        "CREATE ALIGNED TIMESERIES root.aligned.db.d1(s1 INT32, s2 INT32);",
        "CREATE ALIGNED TIMESERIES root.aligned.db.d2(s1 INT32, s2 INT32);",
        "INSERT INTO root.aligned.db.d1(time, s1, s2) ALIGNED VALUES(1, 1, null), (2, null, 2);",
        "INSERT INTO root.aligned.db.d2(time, s1, s2) ALIGNED VALUES(1, null, 1);",
        // test group by time
        "CREATE ALIGNED TIMESERIES root.aligned.downsampling.d1(s1 INT32, s2 INT32);",
        "CREATE ALIGNED TIMESERIES root.aligned.downsampling.d2(s1 INT32, s2 INT32);",
        "INSERT INTO root.aligned.downsampling.d1(time, s1, s2) ALIGNED VALUES(0, 0, null), (1, null, 1), "
            + "(2, null, 2), (4,4,null), (5,5,5), (7,null,7), (8,8,8), (9,null,9);",
        "INSERT INTO root.aligned.downsampling.d2(time, s1, s2) ALIGNED VALUES(0,null,0) (1, 1, null), (2,2,null), "
            + "(4,null,4), (5,5,5), (7,7,null), (8,8,8);",

        // test group by variation
        "CREATE ALIGNED TIMESERIES root.aligned.variation.d1(state INT32, s1 INT32);",
        "CREATE ALIGNED TIMESERIES root.aligned.variation.d2(state INT32, s1 INT32);",
        "INSERT INTO root.aligned.variation.d1(time, state, s1) ALIGNED VALUES(0,0,0), (1,0,null), (2,null,2), (3,0,3), (4,0,null),(5,1,null),(6,1,6);",
        "INSERT INTO root.aligned.variation.d2(time, state, s1) ALIGNED VALUES(0,0,null), (1,null,1), (2,1,2), (3,1,3), (4,1,null), (6,1,null);",
        // test group by session
        "CREATE ALIGNED TIMESERIES root.aligned.session.d1(state INT32, s1 INT32);",
        "CREATE ALIGNED TIMESERIES root.aligned.session.d2(state INT32, s1 INT32);",
        "INSERT INTO root.aligned.session.d1(time, state, s1) ALIGNED VALUES(0,0,0), (1,0,null), (20,0,2), (23,0,3),(40,0,null),(55,1,null),(56,1,6);",
        "INSERT INTO root.aligned.session.d2(time, state, s1) ALIGNED VALUES(0,0,null), (1,null,1), (20,1,2), (23,1,3), (40,1,null), (56,1,null);",

        // test group by condition
        "CREATE ALIGNED TIMESERIES root.aligned.condition.d1(state INT32, s1 INT32, s2 INT32);",
        "CREATE ALIGNED TIMESERIES root.aligned.condition.d2(state INT32, s1 INT32);",
        "INSERT INTO root.aligned.condition.d1(time, state, s1, s2) ALIGNED VALUES(0,0,0,0), (1,1,null,1), (20,null,null,2), (23,1,3,3),(40,0,null,null),(55,1,null,null),(56,1,null,6);",
        "INSERT INTO root.aligned.condition.d2(time, state, s1) ALIGNED VALUES(0,0,null), (1,null,1), (20,1,2), (23,1,3), (40,1,null), (56,1,null);",
        // test having
        "CREATE ALIGNED TIMESERIES root.aligned.having.d1(s1 INT32, s2 INT32);",
        "CREATE ALIGNED TIMESERIES root.aligned.having.d2(s1 INT32, s2 INT32);",
        "INSERT INTO root.aligned.having.d1(time, s1, s2) ALIGNED VALUES(0,0,null), (1,null,1), (2,null,2), (4,4,null), (5,5,5), (7,null,7), (8,8,8), (9,null,9);",
        "INSERT INTO root.aligned.having.d2(time, s1, s2) ALIGNED VALUES(0,null,0), (1,1,null), (2,2,null), (4,null,4), (5,5,5), (7,7,null), (8,8,8), (9,9,null);",
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
    resultSetEqualTest("SELECT COUNT_TIME(*) FROM root.aligned.db.**;", expectedHeader, retArray);

    expectedHeader = new String[] {"count_time(*)"};
    retArray = new String[] {"2,"};
    resultSetEqualTest(
        "SELECT COUNT_TIME(*) FROM root.aligned.db.d1, root.aligned.db.d2;",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {"Device,count_time(*)"};
    retArray = new String[] {"root.aligned.db.d1,2,", "root.aligned.db.d2,1,"};
    resultSetEqualTest(
        "select count_time(*) from root.aligned.db.** align by device;", expectedHeader, retArray);

    expectedHeader = new String[] {"Device,count_time(*)"};
    retArray = new String[] {"root.aligned.db.d1,2,", "root.aligned.db.d2,1,"};
    resultSetEqualTest(
        "select count_time(*) from root.aligned.db.d1,root.aligned.db.d2 align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void groupByTimeTest() {
    // align by time
    String[] expectedHeader = new String[] {"Time,count_time(*)"};
    String[] retArray = new String[] {"0,2,", "2,1,", "4,2,", "6,1,", "8,2,"};
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.aligned.downsampling.** GROUP BY([0, 10), 2ms);",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.aligned.downsampling.d1,root.aligned.downsampling.d2 GROUP BY([0, 10), 2ms);",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.aligned.downsampling.d2,root.aligned.downsampling.d1 GROUP BY([0, 10), 2ms);",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Time,count_time(*)"};
    retArray = new String[] {"0,2,", "2,1,", "4,2,", "6,1,", "8,2,"};
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.aligned.downsampling.d1 GROUP BY([0, 10), 2ms);",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {"Time,Device,count_time(*)"};
    retArray =
        new String[] {
          "0,root.aligned.downsampling.d1,2,",
          "2,root.aligned.downsampling.d1,1,",
          "4,root.aligned.downsampling.d1,2,",
          "6,root.aligned.downsampling.d1,1,",
          "8,root.aligned.downsampling.d1,2,",
          "0,root.aligned.downsampling.d2,2,",
          "2,root.aligned.downsampling.d2,1,",
          "4,root.aligned.downsampling.d2,2,",
          "6,root.aligned.downsampling.d2,1,",
          "8,root.aligned.downsampling.d2,1,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.aligned.downsampling.** GROUP BY([0, 10), 2ms) ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.aligned.downsampling.d1,root.aligned.downsampling.d2 GROUP BY([0, 10), 2ms) ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.aligned.downsampling.d2,root.aligned.downsampling.d1 GROUP BY([0, 10), 2ms) ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // test sort
    expectedHeader = new String[] {"Time,Device,count_time(*)"};
    retArray =
        new String[] {
          "2,root.aligned.downsampling.d1,1,",
          "6,root.aligned.downsampling.d1,1,",
          "2,root.aligned.downsampling.d2,1,",
          "6,root.aligned.downsampling.d2,1,",
          "8,root.aligned.downsampling.d2,1,",
          "0,root.aligned.downsampling.d1,2,",
          "4,root.aligned.downsampling.d1,2,",
          "8,root.aligned.downsampling.d1,2,",
          "0,root.aligned.downsampling.d2,2,",
          "4,root.aligned.downsampling.d2,2,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.aligned.downsampling.** GROUP BY([0, 10), 2ms) ORDER BY count_time(*) ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void groupByVariationTest() {
    // align by time
    String[] expectedHeader = new String[] {"Time,__endTime,count_time(*)"};
    String[] retArray = new String[] {"0,1,2,", "2,2,1,", "3,4,2,", "5,6,2,"};
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.aligned.variation.d1 group by variation(state, 0, ignoreNull=False);",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {"Time,Device,__endTime,count_time(*)"};
    retArray =
        new String[] {
          "0,root.aligned.variation.d1,1,2,",
          "2,root.aligned.variation.d1,2,1,",
          "3,root.aligned.variation.d1,4,2,",
          "5,root.aligned.variation.d1,6,2,",
          "0,root.aligned.variation.d2,0,1,",
          "1,root.aligned.variation.d2,1,1,",
          "2,root.aligned.variation.d2,6,4,",
        };
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.aligned.variation.** "
            + "group by variation(state, 0, ignoreNull=False) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.aligned.variation.d1,root.aligned.variation.d2 "
            + "group by variation(state, 0, ignoreNull=False) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.aligned.variation.d2,root.aligned.variation.d1 "
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
        "select __endTime, count_time(*) from root.aligned.session.** group by session(10ms);",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.aligned.session.d1,root.aligned.session.d2 group by session(10ms);",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.aligned.session.d2,root.aligned.session.d1 group by session(10ms);",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {"Time,Device,__endTime,count_time(*)"};
    retArray =
        new String[] {
          "0,root.aligned.session.d1,1,2,",
          "20,root.aligned.session.d1,23,2,",
          "40,root.aligned.session.d1,40,1,",
          "55,root.aligned.session.d1,56,2,",
          "0,root.aligned.session.d2,1,2,",
          "20,root.aligned.session.d2,23,2,",
          "40,root.aligned.session.d2,40,1,",
          "56,root.aligned.session.d2,56,1,",
        };
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.aligned.session.** group by session(10ms) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.aligned.session.d1,root.aligned.session.d2 group by session(10ms) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select __endTime, count_time(*) from root.aligned.session.d2,root.aligned.session.d1 group by session(10ms) align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void groupByConditionTest() {
    // align by time
    String[] expectedHeader = new String[] {"Time,count_time(*)"};
    String[] retArray = new String[] {"55,2,"};
    resultSetEqualTest(
        "select count_time(*) from root.aligned.condition.d1 group by condition(state=1, KEEP>=2, ignoreNull=false);",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {"Time,Device,count_time(*)"};
    retArray = new String[] {"55,root.aligned.condition.d1,2,", "20,root.aligned.condition.d2,4,"};
    resultSetEqualTest(
        "select count_time(*) from root.aligned.condition.** group by condition(state=1, KEEP>=2, ignoreNull=false) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select count_time(*) from root.aligned.condition.d1,root.aligned.condition.d2 group by condition(state=1, KEEP>=2, ignoreNull=false) align by device;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "select count_time(*) from root.aligned.condition.d2,root.aligned.condition.d1 group by condition(state=1, KEEP>=2, ignoreNull=false) align by device;",
        expectedHeader,
        retArray);
  }
}

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
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualWithDescOrderTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.avg;
import static org.apache.iotdb.itbase.constant.TestConstant.count;
import static org.apache.iotdb.itbase.constant.TestConstant.extreme;
import static org.apache.iotdb.itbase.constant.TestConstant.firstValue;
import static org.apache.iotdb.itbase.constant.TestConstant.lastValue;
import static org.apache.iotdb.itbase.constant.TestConstant.maxTime;
import static org.apache.iotdb.itbase.constant.TestConstant.maxValue;
import static org.apache.iotdb.itbase.constant.TestConstant.minTime;
import static org.apache.iotdb.itbase.constant.TestConstant.minValue;
import static org.apache.iotdb.itbase.constant.TestConstant.sum;
import static org.apache.iotdb.itbase.constant.TestConstant.timeDuration;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAggregationOptimizeIT {
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.test",
        "CREATE TIMESERIES root.test.1region_d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.1region_d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.1region_d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.1region_d2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.2region_d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.2region_d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.2region_d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.2region_d2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.test.1region_d1(timestamp,s1,s2) values(1, 1, 1)",
        "INSERT INTO root.test.1region_d2(timestamp,s1,s2) values(1, 1, 1)",
        "INSERT INTO root.test.1region_d1(timestamp,s1,s2) values(2, 2, 2)",
        "INSERT INTO root.test.1region_d2(timestamp,s1,s2) values(2, 2, 2)",
        "INSERT INTO root.test.2region_d1(timestamp,s1,s2) values(1, 1, 1)",
        "INSERT INTO root.test.2region_d2(timestamp,s1,s2) values(1, 1, 1)",
        "INSERT INTO root.test.2region_d1(timestamp,s1,s2) values(1000000000000, 1, 1)",
        "INSERT INTO root.test.2region_d2(timestamp,s1,s2) values(1000000000000, 1, 1)",
        "flush",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testEachSeriesOneRegion() {
    String[] expectedHeader =
        new String[] {
          count("root.test.1region_d1.s1"),
          count("root.test.1region_d2.s1"),
          count("root.test.1region_d1.s2"),
          count("root.test.1region_d2.s2")
        };
    String[] retArray = new String[] {"2,2,2,2,"};
    resultSetEqualWithDescOrderTest(
        "select count(s1), count(s2) from root.test.1region_d1, root.test.1region_d2",
        expectedHeader,
        retArray);
  }

  @Test
  public void testSomeSeriesOneRegion() {
    String[] expectedHeader =
        new String[] {count("root.test.1region_d1.s1"), count("root.test.2region_d2.s1")};
    String[] retArray = new String[] {"2,2,"};
    resultSetEqualWithDescOrderTest(
        "select count(s1) from root.test.1region_d1, root.test.2region_d2",
        expectedHeader,
        retArray);
  }

  @Test
  public void testAggregationResultConcatOneRegion() {
    String[] expectedHeader =
        new String[] {count("root.test.2region_d1.s1"), count("root.test.2region_d1.s2")};
    String[] retArray = new String[] {"2,2,"};
    resultSetEqualWithDescOrderTest(
        "select count(s1),count(s2) from root.test.2region_d1", expectedHeader, retArray);
  }

  @Test
  public void mixTest() {
    String[] expectedHeader =
        new String[] {
          count("root.test.2region_d1.s1"),
          count("root.test.2region_d2.s1"),
          count("root.test.1region_d2.s1"),
          count("root.test.1region_d1.s1"),
          count("root.test.2region_d1.s2"),
          count("root.test.2region_d2.s2"),
          count("root.test.1region_d2.s2"),
          count("root.test.1region_d1.s2")
        };
    String[] retArray = new String[] {"2,2,2,2,2,2,2,2,"};
    resultSetEqualWithDescOrderTest(
        "select count(s1),count(s2) from root.test.**", expectedHeader, retArray);

    expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          sum("root.test.2region_d1.s1"),
          sum("root.test.2region_d2.s1"),
          sum("root.test.1region_d2.s1"),
          sum("root.test.1region_d1.s1"),
          sum("root.test.2region_d1.s2"),
          sum("root.test.2region_d2.s2"),
          sum("root.test.1region_d2.s2"),
          sum("root.test.1region_d1.s2")
        };
    retArray =
        new String[] {
          "1,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,", "2,null,null,2.0,2.0,null,null,2.0,2.0,"
        };
    resultSetEqualWithDescOrderTest(
        "select sum(s1),sum(s2) from root.test.** group by ([1,3), 1ms)", expectedHeader, retArray);
  }

  @Test
  public void testStaticAggregator() {
    String[] expectedHeader =
        new String[] {
          count("root.test.1region_d1.s1"),
          sum("root.test.1region_d1.s1"),
          avg("root.test.1region_d1.s1"),
          extreme("root.test.1region_d1.s1"),
          maxValue("root.test.1region_d1.s1"),
          minValue("root.test.1region_d1.s1"),
          firstValue("root.test.1region_d1.s1"),
          lastValue("root.test.1region_d1.s1"),
          maxTime("root.test.1region_d1.s1"),
          minTime("root.test.1region_d1.s1"),
          count("root.test.2region_d1.s1"),
          timeDuration("root.test.2region_d1.s1")
        };
    String[] retArray = new String[] {"2,3.0,1.5,2,2,1,1,2,2,1,2,999999999999,"};
    resultSetEqualWithDescOrderTest(
        "select count(1region_d1.s1),sum(1region_d1.s1),avg(1region_d1.s1),"
            + "extreme(1region_d1.s1),max_value(1region_d1.s1),min_value(1region_d1.s1),"
            + "first_value(1region_d1.s1),last_value(1region_d1.s1),max_time(1region_d1.s1),"
            + "min_time(1region_d1.s1),count(2region_d1.s1),time_duration(2region_d1.s1) from root.test",
        expectedHeader,
        retArray);
  }
}
